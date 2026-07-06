# dj.concurrency

Smarter futures for Clojure. `dj.concurrency` helps you manage unreliable, asynchronous work (like flaky API calls) and recover gracefully when things break, especially at the REPL.

> Status: early. The core (`dj.concurrency`) is working and tested; the API may still change. Zero runtime dependencies.

## The payoff

Here is a pipeline that takes a long document, breaks it into chunks, summarizes them concurrently, and derives action items:

```clojure
(def sup (c/create-supervisor {:name "llm"}))

;; A helper that returns a future
(defn ask [prompt] (c/submit sup {:prompt prompt} #(call-llm prompt)))

(defn analyze [doc]
  (let [chunks    (partition-all 2000 doc)                  ; e.g. ~50 chunks
        
        ;; Submit every chunk, then wait for all results. 
        ;; They run concurrently on virtual threads.
        summaries (->> chunks
                       (mapv #(ask (str "Summarize:\n" (apply str %))))
                       (mapv deref))
        
        ;; Combine the summaries, then extract data
        overview     @(ask (str "Combine these summaries:\n" (clojure.string/join "\n\n" summaries)))
        action-items @(ask (str "Extract action items:\n" overview))
        title        @(ask (str "Give a one-line title:\n" overview))]
    {:title title, :overview overview, :action-items action-items}))

(analyze big-document)
;;=> {:title "...", :overview "...", :action-items "..."}
```

This looks like standard, synchronous Clojure code. Yet, behind the scenes, every single one of those API calls is:
- **Retried** automatically if it hits a 503 error.
- **Throttled** collectively if *any* of the calls hit a rate limit.
- **Parked** safely if it completely fails, leaving `analyze` paused at the `@` symbol rather than crashing and destroying the 49 other successful summaries.

## The approach: manageable futures

`dj.concurrency` splits async work into three parts:

1. A **consumer** that just calls `deref` (`@`) on a future and waits for the result.
2. A **worker** (a Java 21 virtual thread) that runs your function.
3. A **supervisor** that manages the lifecycle: retries, throttling, parking, and recovery.

Instead of writing retry loops, you give the supervisor a context map and a standard Clojure function. It gives you back a **future**. 

If your function fails, the supervisor handles the retry and throttling rules completely outside of your business logic. If it fails too many times and can't recover, the supervisor **parks** the task. Your consumer safely stays blocked—it *doesn't* crash—allowing you to inspect the context at the REPL, fix the issue, and either retry the work or manually deliver a result. Once you do, the blocked code resumes as if nothing happened.

## The problem

Imagine calling an LLM API (or any flaky external service) from the REPL. The *happy path* is easy—send a prompt, get a completion. But in the real world, things get messy quickly:

- The endpoint **randomly drops connections** (503s) → you have to write a retry loop.
- It **rate-limits you** (429s) → you have to add backoff logic, ideally coordinating across *all* active requests.
- Something genuinely breaks → your `try/catch` block swallows the context, your REPL session moves on, and you lose the state of the active request you wanted to debug.

Before long, your business logic is buried in retry and throttling code. Worse, a failure deep inside a long chain of requests throws an error and destroys all the work that successfully completed around it. 

## Requirements

- **JDK 21+** — uses virtual threads (`Thread/startVirtualThread`).
- **Clojure 1.11+** — uses `random-uuid`.

The library has **zero dependencies** (`:deps {}`). Dev tooling (a Nix flake + a test runner) is provided if you want to work on this repo itself.

## Installation (deps.edn)

Released to [Clojars](https://clojars.org/net.clojars.bmillare/dj.concurrency) (alpha — the API may still change):

```clojure
net.clojars.bmillare/dj.concurrency {:mvn/version "0.1.0-alpha1"}
```

Or track the bleeding edge straight from git:

```clojure
io.github.bmillare/dj.concurrency {:git/sha "<sha>"}
```

Then require it:

```clojure
(require '[dj.concurrency :as c])
```

## Usage

### 1. A single call

To make a single call, hand the supervisor your function and a context map. You get back a future that you can `deref`. There are no try/catch blocks or retry loops here.

```clojure
(def sup (c/create-supervisor {:name "llm"}))

(def f (c/submit sup
                 {:prompt "summarize the meeting"}
                 (fn [] (call-llm "summarize the meeting"))))

(str "Report: " @f)
;;=> "Report: <completion>"
```

**The deref contract.** `@f` (bare `deref`) blocks until the task reaches a
**terminal** state: `:resolved` → the value, `:aborted` → it throws. A task that
exhausts its retries **parks** (§4), and a parked task never becomes terminal on
its own — so a bare `@f` on it **blocks forever**. In any code that might see a
park, use a timeout instead: `(deref f timeout-ms timeout-val)`, or run a
co-supervisor that services parks.

Combining many of these into a pipeline is where it pays off—see [The payoff](#the-payoff) above.

### 2. Control retries by throwing data

If your function throws an error, the supervisor retries it with exponential backoff. Your `deref` simply takes a bit longer to return:

```clojure
;; The function fails twice with a 503, then succeeds on the 3rd try
@(c/submit sup {:prompt "extract action items"} flaky-call)
;;=> "<completion>"   
```

How does the supervisor know *when* to retry? **By looking at the exceptions you throw.** 

When you write your worker function, you don't write retry logic. Instead, you just throw `ex-info`, and put data inside it. The supervisor reads that data to decide what to do next:

```clojure
(defn call-llm [prompt]
  (let [resp (http/post endpoint {:body prompt})]
    (case (:status resp)
      200 (:body resp)                                  ; Success: return the value
      
      ;; Rate limit: tell the supervisor to throttle based on the header
      429 (throw (ex-info "rate limited"
                          {:status 429                  
                           :retry-after (header-ms resp "retry-after")}))
                           
      ;; Bad request: this is a code bug, retrying won't help
      (400 401 403)                                     
          (throw (ex-info "bad request"
                          {:type :business-error}))     
                          
      ;; 503s, timeouts, etc.: standard errors
      (throw (ex-info (str "transient " (:status resp)) {:status (:status resp)})))))
```

The supervisor's default policy looks at the keys in your `ex-data`:
- If it sees `{:type :business-error}`, it aborts immediately and **parks** the task for you to inspect.
- If it sees `{:status 429}`, it **throttles** the entire supervisor (pausing all other requests for the duration of `:retry-after`).
- If it sees anything else, it assumes a transient error and **retries** up to the max attempt limit (default is 3), and then parks.

You can customize this entirely. If you want to trigger special behavior (like falling back to a cheaper model, or refreshing an auth token), just have your function throw a specific key in `ex-info` and configure the supervisor to look for it (see *Customizing policy* below).

You can tweak the default settings globally when creating the supervisor, or per-task via the context map (using the `:dj.concurrency/` namespace):

```clojure
(c/create-supervisor {:name        "llm"
                      :max-attempts 5
                      :backoff-fn  (fn [attempts] (* 250 attempts))}) ; 250ms linear backoff
```

### 3. Rate limits pause everything

If an API returns a `429` (Rate Limited), the supervisor pauses. If you have other active requests trying to run, they won't hammer the API. They will sit in a queue until the timeout passes, and then resume automatically:

```clojure
;; If req-1 gets a 429, req-2 and req-3 queue up automatically
(mapv deref [req-1 req-2 req-3])  

;; You can also manually lift a throttle from the REPL:
(c/clear-throttle sup)
```

### 4. Fix broken tasks live at the REPL

When a task fails all its retries, it **parks**. The code waiting on it stays paused, preserving all your local state. You can debug it directly from the REPL:

```clojure
(c/parked-tasks sup)            ; See what's stuck
(c/task f)                      ; View the full task map: status, context, and error
(:context (c/task f))           ;=> {:prompt "draft the release notes", :dj.concurrency/attempts 3}
(ex-message (:error (c/task f)))

;; Fix the issue, then choose how to resume without crashing the waiting code:
(c/deliver-result f "MOCK: ...")  ; Provide a mock result, unblocking the waiting code
(c/retry f)                       ; Re-run the function (useful if you just hot-reloaded a fix)
(c/abort f (ex-info "give up" {})); Force it to fail
(c/cancel f)                      ; Drop it entirely
```

You can try out an interactive, runnable demo of all these scenarios by running:

```bash
clojure -M:repl -e "(require 'dj.concurrency.llm-demo)(dj.concurrency.llm-demo/-main)"
```
*(Source: [`dev/dj/concurrency/llm_demo.clj`](dev/dj/concurrency/llm_demo.clj))*

### 5. Cleaning up

Task state is retained after a task finishes so your REPL recovery story keeps working—you can always inspect a resolved or aborted task's context and error. When you're done and want to reclaim memory, just prune them:

```clojure
(c/prune sup)              ; drop all resolved, aborted, or cancelled tasks
(c/prune sup #{:resolved}) ; or restrict to a subset of statuses
```

### 6. Bound concurrency to a slow/limited backend (recipe)

Retries and throttling (§2, §3) both *react after* a request has already timed out. But if the real problem is that your backend can only serve a few requests at once — a local LLM with one worker, a small connection pool, a rate-limited-by-concurrency API — the cleaner fix is to **not over-subscribe it in the first place**. Firing 5 requests at a 1-worker backend just makes 4 of them wait past their timeout; retrying then piles *more* work onto the already-saturated worker and makes it worse.

Bounding in-flight work is a **different axis** from retry/backoff, and today it's a short consumer-side recipe — one semaphore sized to the backend's real concurrency:

```clojure
(import '[java.util.concurrent Semaphore])

;; ONE gate per backend, sized to the backend's real concurrency:
;;   a single-worker local model => 1 permit;  a pool of N workers => N permits.
(def gate (Semaphore. 1))

(defn bounded [work-fn]
  (.acquire gate)                       ; only `permits` requests reach the backend at once
  (try @(c/submit sup {} work-fn)       ; so nothing times out from pile-up
       (finally (.release gate))))

;; Fan out however you like — the gate caps in-flight work at the permit count:
(mapv #(future (bounded %)) work-fns)
```

Three things make or break this recipe:

- **Size the permits to the backend's worker count.** Too many permits re-creates the pile-up (over-subscription); too few just leaves workers idle. Permits = workers is the sweet spot.
- **Use *one* gate per backend, shared by every call site.** The bound is a property of the *backend*, not the call site: two independent callers each locally capped at 1 still put 2 requests on a shared 1-worker backend. Only a single shared semaphore actually bounds it.
- **Don't let a parked task hold its permit forever.** The `bounded` helper holds the permit across the whole `deref` (deliberately — that keeps the bound intact across retries). But a task that exhausts its retries **parks**, and a bare `@` on a parked task blocks forever (see §4), wedging the permit. So pair this recipe with either a `(deref f timeout ...)` or a co-supervisor that services parks (§4) — the concurrency bound (prevention) and park-recovery (cure) compose cleanly.

> This is a documented recipe, not yet library surface. If you find yourself needing one gate shared across many call sites or supervisors, that's the signal it should become a first-class per-backend concurrency limit — tell us about your workload.

## Durable results / crash recovery

Long pipelines are expensive to lose. If the JVM dies after summarizing 49 of 50 chunks, you don't want to pay for those 49 LLM calls again on restart. `dj.concurrency` gives you an **opt-in, crash-safe memo table** for task results.

The model is deliberately simple: **deterministic re-run + durable memo table**, *not* process resumption. After a crash you re-run the same workflow from the top; any task whose result was already recorded resolves **instantly from the journal** instead of re-executing. This only requires that your orchestration code derives the *same key* for the same work on re-run.

It's opt-in from two sides, and if either side opts out you get exactly today's behavior:

- The **supervisor** opts in with a `:store` (anything satisfying the 3-fn `dj.concurrency.store/ResultStore` protocol).
- A **task** opts in by putting `:dj.concurrency/durable-key` in its context.

```clojure
(require '[dj.concurrency :as c]
         '[dj.concurrency.store :as store])

;; An in-memory store (great for tests / pure process memoization):
(def sup (c/create-supervisor {:name "llm" :store (store/atom-store)}))

;; Give each memoizable task a stable key derived from its inputs. A key is just
;; EDN — derive your own, e.g. [:summarize prompt]:
(c/submit sup
          {:prompt p
           :dj.concurrency/durable-key [:summarize p]}
          #(call-llm p))
```

On execution the worker checks the store *before* running your function. A **hit** resolves the future with the cached value (the task is annotated `:cached? true`, visible via `(c/task f)`). A **miss** runs the function, **durably persists the result before the future resolves**, then resolves it — so once `@f` returns, the result is on disk.

Keys are stored **verbatim** — there's no hashing step, so a durable journal stays introspectable (you can read it and see exactly which inputs produced which result). Clojure value-equality decides hits, so map key order is irrelevant. Identical inputs dedupe by design; add a run-id to the key (e.g. `[:summarize run-id p]`) if you want distinct runs to re-execute. If your inputs are large and you don't need the introspection, hash them yourself and use the digest as the key.

### Durable across restarts with dj.recorder

`atom-store` isn't durable. For crash recovery, back the store with a journal such as [`dj.recorder`](https://github.com/bmillare/dj.recorder). The protocol ships here (zero dependencies); the `dj.recorder`-backed adapter lives with `dj.recorder` (namespace `dj.recorder.concurrency-store`) so `dj.concurrency` stays dependency-free:

```clojure
(require '[dj.recorder :as r]
         '[dj.recorder.concurrency-store :as cs])

(def db  (r/open "results.edn"))
(def sup (c/create-supervisor {:name "llm"
                               :store (cs/recorder-store db [:results "run-42"])}))
;; run the pipeline; kill the JVM mid-run; restart; re-run the SAME pipeline with
;; the SAME run-id — completed chunks resolve as :cached? true, hitting no API.
```

### Notes & guarantees

- **Store failures never fail a task.** Any throw from the store degrades to no-cache (the function runs normally) and is reported through your `:event-tap` as `:store-lookup-failed` / `:store-record-failed`. Persisted-before-resolved is the guarantee on the happy path.
- **`nil` and `false` are cached.** Results travel in a `{:result r}` envelope, so a cached `nil`/`false` is a genuine hit, not a miss.
- **Side-effecting tasks just omit the key.** No `:store` or no `:dj.concurrency/durable-key` → identical to today; UI updates and other effects shouldn't be memoized.
- **Failures write nothing.** A keyed task that throws parks/retries exactly as before; only successful results are recorded.
- **Stale memo?** `c/retry` on a task whose key already has an entry returns the memo. If the cached value is stale (e.g. the prompt template changed), `(c/evict! sup k)` first, then retry.
- **REPL mocks are never persisted, intentionally.** `deliver-result` flows through the resolve path, not the worker, so a mock never poisons the durable cache.

## API Quick Reference

**Setup & Execution:**
- `create-supervisor` `{:policy :event-tap :name :store :backoff-fn :max-attempts ...}` → Returns a supervisor map (plays nicely with Component/Integrant). Pass `:store` (a `dj.concurrency.store/ResultStore`) to enable durable memoization.
- `submit` `[sup context function]` → Returns a future (supports `@`, 3-arg `deref` timeouts, and `realized?`). Put `:dj.concurrency/durable-key` in `context` to memoize the result.
- `stop!` `[sup]` or `[sup mode]` → Stops the supervisor (modes: `:abort-pending` or `:drop`).
- `wait-for-shutdown` `[sup]` → Returns a promise that resolves when everything is fully stopped.

**Durable memoization (`dj.concurrency.store`):**
- `ResultStore` protocol — `lookup`, `record!`, `evict!`. Implement it over any journal.
- `atom-store` → non-durable in-memory store (tests / process memoization).
- Keys are plain EDN, stored verbatim (no helper needed) — e.g. `[:summarize prompt]`.

**Inspection (REPL / reconcile loop):**
- `state`, `tasks`, `task`, `parked-tasks`, `tasks-by-status` (`{status [task ...]}` — the one-call lens for a poll loop)

**Intervention (REPL):**
- `deliver-result`, `retry`, `abort`, `cancel`, `clear-throttle`, `prune`, `evict!`

## Logging

`dj.concurrency` doesn't force a logging framework on you. Internal events are sent to an `:event-tap` you define when creating the supervisor.

By default, it uses **`default-event-tap`**: a loud dev breadcrumb that prints every event to `*err*` the instant it is emitted, so a run never parks in silence. It's a development aid, not a production logging strategy — in production, pass your own `:event-tap`:

```clojure
;; Send events to your app's actual logger:
(c/create-supervisor {:event-tap (fn [entry] (my-logger/info entry))})

;; Or restore the old silent-by-default behavior (routes to clojure.core/tap>,
;; a no-op unless you register a tap listener with (add-tap ...)):
(c/create-supervisor {:event-tap tap>})
```
*(Log entries look like: `{:level :debug :event :submit-executed :task-id <id> :data <task-id>}`. Every task-scoped event carries a top-level `:task-id` — `(:task-id entry)` is the reliable handle to feed an intervention, whatever the `:data` shape. Genuinely supervisor-scoped events like `:pruned` have no `:task-id`.)*

The `:event-tap` is the supervisor's **event tap** — the one place a running
supervisor's lifecycle transitions escape to observers. The events most worth
watching (they make retry/throttle behavior legible, e.g. a retry-storm against a
saturated backend) are:

| `:event` | `:level` | `:data` | meaning |
|---|---|---|---|
| `:submit-executed` | `:debug` | task-id | task handed to a worker thread |
| `:retry-scheduled` | `:debug` | `{:task-id :attempt :max-attempts :wake-in-ms}` | a transient failure will be retried after a backoff |
| `:throttle-wait` | `:info` | `{:task-id :wake-in-ms}` | a 429 paused the **whole supervisor** for `:wake-in-ms` |
| `:parked` | `:info` | task-id | retries exhausted; the task awaits intervention (see `parked-tasks`) |

Following `:retry-scheduled` in the tap is how you *see* — rather than infer from a
hang — that a slow local/single-worker backend is being retried into deeper
saturation. The rich, authoritative state for a parked task (its `:context`,
`:error`, attempt count) lives in `(parked-tasks sup)`; the tap is a low-latency
doorbell, `parked-tasks` is the source of truth.

## Reacting to events (co-supervision)

To have a program (or coding agent) *react* — retry, abort, notify — rather than
just watch, use two layers. **Build correctness on the poll; use the tap only for
speed.**

- **Poll + reconcile — authoritative.** On a cadence you own, read
  `(tasks-by-status sup)`, decide, and act via the interventions (`retry`,
  `deliver-result`, `abort`, `cancel`, `prune`). This never misses; a run left
  entirely to this loop is *correct*, just less responsive.
- **The tap — responsiveness, lossy.** The `:event-tap` shrinks the latency between
  "task parked" and "you react," but it is best-effort: never build correctness on
  it. A missed edge must be caught by the next reconcile pass.

The tap is a **notification sink**, not the place to act — three reasons that all
point at the same recipe:

1. It runs **synchronously on the supervisor's control thread** — a blocking tap
   wedges the whole supervisor (throttle/admission deadlines stop clearing). Keep it
   fast and non-blocking.
2. It receives an `entry`, not `sup` — and `sup` doesn't exist until
   `create-supervisor` returns — so it *can't* call `(retry sup id)` directly.
3. It's lossy, so acting from it isn't enough anyway.

So: the tap hands attention-worthy entries to your own queue; a servicer virtual
thread (created after `sup`, closing over it) drains the queue and acts — off the
control thread, with `sup` in hand, using `(:task-id entry)` as the handle.

```clojure
(def attention-q (java.util.concurrent.LinkedBlockingQueue.))

;; edge: a pure, non-blocking sink — your own filter is your policy
(def sup (c/create-supervisor
           {:event-tap (fn [entry]
                         (when (#{:parked} (:event entry))     ; what YOU care about
                           (.offer attention-q entry)))}))     ; fast, never blocks

;; servicer: owns sup + may block, off the control thread
(Thread/startVirtualThread
  (fn [] (while true
           (let [entry (.take attention-q)]                    ; blocks safely here
             (c/retry sup (:task-id entry))))))                ; reliable handle

;; level: the authoritative backstop — reconcile on your own cadence
(doseq [t (:parked (c/tasks-by-status sup))]
  (c/retry sup (:task-id t)))
```

Deciding *which* events matter (and what to do about them) is your policy — the
library ships the channel, the poll surface, and the verbs, and stays out of the
judgment.

## Customizing policy

Under the hood, the supervisor uses a state machine. It is driven by a pure function that takes the current state and an event, and returns the next state alongside instructions (like "spawn a thread" or "resolve a future").

Most of the time, you don't need to write a custom policy. You can just pass options like `:backoff-fn`, `:max-attempts`, `:classify-error`, or `:default-throttle-ms` to `create-supervisor`. 

If you *do* need total control over how tasks are managed, you can write your own state transition function and pass it as the `:policy`. Check out `default-policy` and `make-reference-policy` (re-exported from `dj.concurrency`, implemented in [`src/dj/concurrency/policy.clj`](src/dj/concurrency/policy.clj)) to see how the default state machine handles things like success, failure, REPL interventions, and shutdown.

> **Internals:** the code is split into a functional core and an imperative shell — [`dj.concurrency.policy`](src/dj/concurrency/policy.clj) is the pure state machine, [`dj.concurrency.shell`](src/dj/concurrency/shell.clj) is the impure runtime (event queue, virtual threads, the future handle), and [`dj.concurrency`](src/dj/concurrency.clj) is the thin user-facing facade. You only ever require `dj.concurrency`.

## Developing on this repo

```bash
nix develop            # clojure + babashka + jdk 21
clojure -X:test        # run the test suite
clojure -M:repl        # dev REPL (adds dev/ and test/ to the path)
```

## License

See [LICENSE](LICENSE).
