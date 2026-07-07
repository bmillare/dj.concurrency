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

**The deref contract.** `@f` blocks until the task reaches a **terminal** state
(`:resolved` returns the value, `:aborted` throws). A task that exhausts its retries
**parks** (§4) and never becomes terminal on its own, so a bare `@f` on a parked task
**blocks forever** — use `(deref f timeout-ms timeout-val)` or a co-supervisor in any
code that might see a park. (Full contract in the
[in-depth guide](README_AGENTS.md#the-deref-contract-in-full).)

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

You can customize this entirely. If you want to trigger special behavior (like falling back to a cheaper model, or refreshing an auth token), just have your function throw a specific key in `ex-info` and configure the supervisor to look for it (see [Customizing policy](README_AGENTS.md#customizing-policy)).

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

## Going further

The [in-depth guide (`README_AGENTS.md`)](README_AGENTS.md) covers the rest — aimed at contributors, power users, and coding agents:

- **[Bounding concurrency to a slow backend](README_AGENTS.md#bounding-concurrency-to-a-slow-backend-recipe)** — a one-semaphore recipe for when the backend can only serve a few requests at once (a different axis from retry/throttle).
- **[Durable results / crash recovery](README_AGENTS.md#durable-results--crash-recovery)** — an opt-in, crash-safe memo table: after a crash, re-run the same workflow and completed work resolves instantly from a journal instead of re-executing (e.g. no re-paying for 49 of 50 LLM calls).
- **[Logging & the event tap](README_AGENTS.md#logging--the-event-tap)** + the **[full event vocabulary](README_AGENTS.md#the-event-vocabulary-full-reference)** — how lifecycle transitions reach you, and every event the supervisor emits.
- **[Reacting to events (co-supervision)](README_AGENTS.md#reacting-to-events-co-supervision)** — driving retries/aborts programmatically with a poll+tap pattern, plus a tested, copy-paste [`dev/` reference co-supervisor](dev/dj/concurrency/reference_co_supervisor.clj).
- **[Customizing policy](README_AGENTS.md#customizing-policy)** — `:classify-error`, backoff, and writing your own state machine.
- **[Architecture](README_AGENTS.md#architecture-functional-core--imperative-shell)**, **[task lifecycle & statuses](README_AGENTS.md#task-lifecycle--statuses)**, the **[API reference](README_AGENTS.md#api-reference)**, and **[developing on this repo](README_AGENTS.md#developing-on-this-repo)**.

## License

See [LICENSE](LICENSE).
