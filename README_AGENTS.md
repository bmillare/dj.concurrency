# dj.concurrency — in-depth guide

The companion to [`README.md`](README.md). The main README is the *why* and the
happy path; this doc is the *how it works* and the *how to operate it* — the
detail for contributors, coding agents, and power users. It assumes you've read the
main README's [payoff](README.md#the-payoff) and basic [usage](README.md#usage).

Contents:
- [Architecture: functional core + imperative shell](#architecture-functional-core--imperative-shell)
- [Task lifecycle & statuses](#task-lifecycle--statuses)
- [The deref contract (in full)](#the-deref-contract-in-full)
- [Bounding concurrency to a slow backend (recipe)](#bounding-concurrency-to-a-slow-backend-recipe)
- [Durable results / crash recovery](#durable-results--crash-recovery)
- [Logging & the event tap](#logging--the-event-tap)
- [The event vocabulary (full reference)](#the-event-vocabulary-full-reference)
- [Reacting to events (co-supervision)](#reacting-to-events-co-supervision)
- [Customizing policy](#customizing-policy)
- [API reference](#api-reference)
- [Developing on this repo](#developing-on-this-repo)

---

## Architecture: functional core + imperative shell

The code is split so the decision-making is pure and testable and the side effects
are isolated:

- [`dj.concurrency.policy`](src/dj/concurrency/policy.clj) — the **pure state
  machine**. A policy is a pure function `(fn [event state] -> {:directives [...]
  :state new-state})`. It decides *what should happen* (retry, throttle, park,
  resolve) but performs no side effects. Events and directives are positional pairs
  `[type payload-map]`.
- [`dj.concurrency.shell`](src/dj/concurrency/shell.clj) — the **imperative
  runtime**. A single control loop (on a virtual thread) pulls events off a
  `BlockingQueue`, calls the policy, commits the returned state, and executes the
  returned directives (`:execute` spawns a worker virtual thread, `:resolve`/`:abort`
  complete the `CompletableFuture`, `:log` calls your `:event-tap`, etc.).
- [`dj.concurrency`](src/dj/concurrency.clj) — the **thin facade** you actually
  require: `create-supervisor`, `submit`, the inspection/intervention verbs, and the
  future handle.

Why it matters operationally: every state transition is a pure function of
`(event, state)`, so the whole policy is unit-testable with no threads (drive
`default-policy` with an explicit `:now`), and the shell's control loop is
single-threaded — which is exactly why a blocking `:event-tap` is dangerous (see
[Logging](#logging--the-event-tap)).

The single control thread also means task **state is serialized**: you never race
two transitions for the same task, and `(state sup)` is always a coherent snapshot.

---

## Task lifecycle & statuses

Every task carries a `:status`. The poll surface (`tasks`, `task`, `parked-tasks`,
`tasks-by-status`) reports it; interventions move tasks between them.

| `:status` | meaning | terminal? |
|---|---|---|
| `:queued` | waiting — for a throttle window to lift, or (when a concurrency bound is set) for an admission permit | no |
| `:running` | executing on a worker virtual thread | no |
| `:waiting-retry` | a transient failure or a 429; will re-run when `:wake-at` arrives (a 429 also tags `:throttle?`) | no |
| `:parked` | retries exhausted; awaits REPL/co-supervisor intervention | no |
| `:resolved` | succeeded — `@f` returns the value | **yes** |
| `:aborted` | failed — `@f` throws | **yes** |
| `:cancelled` | dropped via `cancel` | **yes** |

The three terminal statuses (`:resolved :aborted :cancelled`) are the set
`dj.concurrency.policy/terminal-statuses`: safe to prune, safe to drop the future.

Task-map fields you can read from `(task f)` / `(tasks sup)`:
`:task-id :status :context :closure :submitted-at :wake-at :throttle?
:admission-waiting? :error :cached?`.

---

## The deref contract (in full)

`@f` (bare `deref`) blocks until the task reaches a **terminal** state: `:resolved`
→ the value, `:aborted` → it throws. A task that exhausts its retries **parks**, and
a parked task never becomes terminal on its own — so a bare `@f` on it **blocks
forever**.

In any code that might see a park, use a timeout — `(deref f timeout-ms
timeout-val)` — or run a co-supervisor that services parks (see
[Reacting to events](#reacting-to-events-co-supervision)). `realized?` is true only
for terminal tasks, so it's a safe non-blocking check.

This is the one correctness gotcha of the "looks synchronous" style: parking is what
lets a deep failure *not* destroy the surrounding successful work, but the price is
that the blocked `@f` needs either a human/agent to service the park or a timeout.

---

## Bounding concurrency to a slow backend (recipe)

Retries and throttling (see the main README's [§2](README.md#2-control-retries-by-throwing-data)
and [§3](README.md#3-rate-limits-pause-everything)) both *react after* a request has
already timed out. But if the real problem is that your backend can only serve a few
requests at once — a local LLM with one worker, a small connection pool, a
rate-limited-by-concurrency API — the cleaner fix is to **not over-subscribe it in
the first place**. Firing 5 requests at a 1-worker backend just makes 4 of them wait
past their timeout; retrying then piles *more* work onto the already-saturated worker
and makes it worse.

Bounding in-flight work is a **different axis** from retry/backoff, and today it's a
short consumer-side recipe — one semaphore sized to the backend's real concurrency:

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

- **Size the permits to the backend's worker count.** Too many permits re-creates the
  pile-up (over-subscription); too few just leaves workers idle. Permits = workers is
  the sweet spot.
- **Use *one* gate per backend, shared by every call site.** The bound is a property
  of the *backend*, not the call site: two independent callers each locally capped at
  1 still put 2 requests on a shared 1-worker backend. Only a single shared semaphore
  actually bounds it.
- **Don't let a parked task hold its permit forever.** The `bounded` helper holds the
  permit across the whole `deref` (deliberately — that keeps the bound intact across
  retries). But a task that exhausts its retries **parks**, and a bare `@` on a parked
  task blocks forever (see [the deref contract](#the-deref-contract-in-full)), wedging
  the permit. So pair this recipe with either a `(deref f timeout ...)` or a
  co-supervisor that services parks — the concurrency bound (prevention) and
  park-recovery (cure) compose cleanly.

> This is a documented recipe, not yet library surface. (There is experimental,
> internal support for an admission bound via a `:max-in-flight` policy option — it
> emits `:admission-wait`/`:admission-granted` events — but it is not yet a
> committed public API.) If you find yourself needing one gate shared across many
> call sites or supervisors, that's the signal it should become a first-class
> per-backend concurrency limit — tell us about your workload.

---

## Durable results / crash recovery

Long pipelines are expensive to lose. If the JVM dies after summarizing 49 of 50
chunks, you don't want to pay for those 49 LLM calls again on restart.
`dj.concurrency` gives you an **opt-in, crash-safe memo table** for task results.

The model is deliberately simple: **deterministic re-run + durable memo table**,
*not* process resumption. After a crash you re-run the same workflow from the top;
any task whose result was already recorded resolves **instantly from the journal**
instead of re-executing. This only requires that your orchestration code derives the
*same key* for the same work on re-run.

It's opt-in from two sides, and if either side opts out you get exactly today's
behavior:

- The **supervisor** opts in with a `:store` (anything satisfying the 3-fn
  `dj.concurrency.store/ResultStore` protocol).
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

On execution the worker checks the store *before* running your function. A **hit**
resolves the future with the cached value (the task is annotated `:cached? true`,
visible via `(c/task f)`). A **miss** runs the function, **durably persists the
result before the future resolves**, then resolves it — so once `@f` returns, the
result is on disk.

Keys are stored **verbatim** — there's no hashing step, so a durable journal stays
introspectable (you can read it and see exactly which inputs produced which result).
Clojure value-equality decides hits, so map key order is irrelevant. Identical inputs
dedupe by design; add a run-id to the key (e.g. `[:summarize run-id p]`) if you want
distinct runs to re-execute. If your inputs are large and you don't need the
introspection, hash them yourself and use the digest as the key.

### Durable across restarts with dj.recorder

`atom-store` isn't durable. For crash recovery, back the store with a journal such as
[`dj.recorder`](https://github.com/bmillare/dj.recorder). The protocol ships here
(zero dependencies); the `dj.recorder`-backed adapter lives with `dj.recorder`
(namespace `dj.recorder.concurrency-store`) so `dj.concurrency` stays
dependency-free:

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

- **Store failures never fail a task.** Any throw from the store degrades to no-cache
  (the function runs normally) and is reported through your `:event-tap` as
  `:store-lookup-failed` / `:store-record-failed`. Persisted-before-resolved is the
  guarantee on the happy path.
- **`nil` and `false` are cached.** Results travel in a `{:result r}` envelope, so a
  cached `nil`/`false` is a genuine hit, not a miss.
- **Side-effecting tasks just omit the key.** No `:store` or no
  `:dj.concurrency/durable-key` → identical to today; UI updates and other effects
  shouldn't be memoized.
- **Failures write nothing.** A keyed task that throws parks/retries exactly as
  before; only successful results are recorded.
- **Stale memo?** `c/retry` on a task whose key already has an entry returns the memo.
  If the cached value is stale (e.g. the prompt template changed), `(c/evict! sup k)`
  first, then retry.
- **REPL mocks are never persisted, intentionally.** `deliver-result` flows through
  the resolve path, not the worker, so a mock never poisons the durable cache.

---

## Logging & the event tap

`dj.concurrency` doesn't force a logging framework on you. Internal events are sent
to an `:event-tap` you define when creating the supervisor.

By default, it uses **`default-event-tap`**: a loud dev breadcrumb that prints every
event to `*err*` the instant it is emitted, so a run never parks in silence. It's a
development aid, not a production logging strategy — in production, pass your own
`:event-tap`:

```clojure
;; Send events to your app's actual logger:
(c/create-supervisor {:event-tap (fn [entry] (my-logger/info entry))})

;; Or restore the old silent-by-default behavior (routes to clojure.core/tap>,
;; a no-op unless you register a tap listener with (add-tap ...)):
(c/create-supervisor {:event-tap tap>})
```

**The entry contract.** Every entry is a map `{:level kw :event kw :data any}`, plus
a top-level `:task-id` on every *task-scoped* event. `(:task-id entry)` is the
reliable handle to feed an intervention, whatever the `:data` shape (some events put
a bare id in `:data`, others a richer map). Genuinely *supervisor-scoped* events
(`:pruned`, `:unknown-event`, `:shell-iteration-threw`, `:policy-threw`) have no
`:task-id`.

**The tap is a low-latency doorbell; the poll surface is the source of truth.** The
rich, authoritative state for a parked task (its `:context`, `:error`, attempt count)
lives in `(parked-tasks sup)` / `(task f)`; the tap only tells you *sooner*. See
[Reacting to events](#reacting-to-events-co-supervision) for why you should never
build correctness on the tap alone.

**Contract for a custom tap** — it is called **synchronously, on the supervisor's
single control thread**, so it **must be fast and non-blocking**: do no I/O, don't
deref a task, don't acquire locks. A throw is swallowed (a broken tap can't take down
the loop), but a **block wedges the whole supervisor** (the control loop stops
polling *and* stops clearing throttle/admission deadlines). To *act* on an event,
hand it to your own queue/thread — see the recipe below.

---

## The event vocabulary (full reference)

All events the reference policy + shell emit, by `:level`. Task-scoped events carry a
top-level `:task-id`; the `:data` column shows the payload shape.

| `:event` | `:level` | `:data` | task-scoped? | meaning |
|---|---|---|:--:|---|
| `:submit-executed` | `:debug` | task-id | ✓ | task handed to a worker thread (unbounded path) |
| `:submit-queued` | `:debug` | task-id | ✓ | bounded path: submit entered the admission queue |
| `:submit-throttled` | `:info` | task-id | ✓ | submit arrived during a throttle window; queued |
| `:retry-scheduled` | `:debug` | `{:task-id :attempt :max-attempts :wake-in-ms}` | ✓ | a transient failure will be retried after a backoff |
| `:retry-queued` | `:debug` | task-id | ✓ | bounded path: a REPL/co-sup retry re-entered the admission queue |
| `:throttle-wait` | `:info` | `{:task-id :wake-in-ms}` | ✓ | a 429 paused the **whole supervisor** for `:wake-in-ms` |
| `:parked` | `:info` | task-id | ✓ | retries exhausted; the task awaits intervention |
| `:cancelled` | `:info` | task-id | ✓ | task cancelled via `cancel` |
| `:admission-granted` | `:debug` | `{:task-id :in-flight :max-in-flight}` | ✓ | (bounded) a permit freed; task admitted to a worker |
| `:admission-wait` | `:info` | `{:task-id :in-flight :max-in-flight}` | ✓ | (bounded) no free permit; task waiting for admission |
| `:late-success` | `:warn` | task-id | ✓ | a `:success` for an already-terminal/unknown task (ignored) |
| `:late-failure` | `:warn` | task-id | ✓ | a `:failed` for an already-terminal/unknown task (ignored) |
| `:illegal-transition` | `:warn` | task-id | ✓ | success/failure arrived in an unexpected state |
| `:already-running` | `:warn` | task-id | ✓ | `retry` on a task that's already running (no-op) |
| `:no-such-task` | `:warn` | task-id | ✓ | intervention referenced an unknown task |
| `:invalid-deliver` | `:warn` | task-id | ✓ | `deliver-result` in a state that doesn't allow it |
| `:invalid-abort` | `:warn` | task-id | ✓ | `abort` in a state that doesn't allow it |
| `:invalid-cancel` | `:warn` | task-id | ✓ | `cancel` in a state that doesn't allow it |
| `:cannot-cancel-running` | `:warn` | task-id | ✓ | `cancel` on a `:running` task (not allowed) |
| `:store-lookup-failed` | `:warn` | `{:key :error}` (+ `:task-id`) | ✓ | store lookup threw; degraded to a cache miss |
| `:store-record-failed` | `:warn` | `{:key :error}` (+ `:task-id`) | ✓ | store persist threw; degraded to no-cache |
| `:pruned` | `:info` | `{:count}` | — | `prune` dropped N terminal tasks |
| `:unknown-event` | `:error` | event-type kw | — | the policy received an event type it doesn't recognize |
| `:policy-threw` | `:error` | `{:event :error}` | — | the policy fn threw; the shell caught it and continued |
| `:shell-iteration-threw` | `:error` | `{:error}` | — | a shell iteration threw; caught, the loop continues |

Following `:retry-scheduled` in the tap is how you *see* — rather than infer from a
hang — that a slow local/single-worker backend is being retried into deeper
saturation.

---

## Reacting to events (co-supervision)

To have a program (or coding agent) *react* — retry, abort, notify — rather than just
watch, use two layers. **Build correctness on the poll; use the tap only for speed.**

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
library ships the channel (`:event-tap`), the poll surface (`tasks-by-status` et al.),
and the verbs (the interventions), and stays out of the judgment.

### `explain-stuck` — a legible read over the parked set

`(explain-stuck sup)` is the pull / level-triggered companion to the push /
edge-triggered tap. Where the tap tells you *when* something parks (lossy, live), this
answers *"what is parked right now, and why?"* off the authoritative poll surface
(never lossy). It's an opinionated **summary** over `parked-tasks` — not a new
primitive — so it makes no policy decisions and can't be wrong about them.

The value is legibility. A raw parked-task map is hostile to read: attempts hide at
`[:context :dj.concurrency/attempts]`, the durable key at
`[:context :dj.concurrency/durable-key]`, the failure is a stack-traced `Throwable`
under `:error`, and printing it dumps closures and nested policy state.
`explain-stuck` condenses each parked task to flat, plain values and returns pure,
pretty-printable data:

```clojure
(explain-stuck sup)
;=> {:parked-count 2
;    :tasks [{:task-id "a3f1…" :attempts 5 :error "upstream 503"
;             :error-type :transient :durable-key "summarize:doc-42" :age-ms 3200}
;            {:task-id "b8c2…" :attempts 5 :error "connection reset"
;             :error-type :transient :durable-key nil :age-ms 900}]}
```

- **`:tasks` is sorted oldest-first** (largest `:age-ms`) — the longest-stuck task, the
  one most likely to need you, reads first.
- **Every field is a value** (string / int / keyword / nil) — the `:error` is
  `ex-message`, never the Throwable; `:error-type` is the classifier tag from
  `ex-data`; `:durable-key` is the store handle for `(evict! sup key)` before a
  `retry` (nil when the task isn't memoized).
- **A map, not a bare vector**, so aggregate fields can be added later without breaking
  callers that destructure `:tasks`.

At a REPL it auto-pretty-prints; a co-supervisor reconciles off the same value:

```clojure
(->> (explain-stuck sup) :tasks
     (filter #(= :transient (:error-type %)))
     (run! #(c/retry sup (:task-id %))))
```

Scope: `:parked` only — the set that will otherwise block `deref` forever.
`:waiting-retry` is self-healing and `:queued`/`:running` are progressing; for the
fuller picture use `tasks-by-status`. `explain-stuck` doesn't push or notify — if you
never call it you learn nothing; that half is the tap's job.

---

## Customizing policy

Under the hood, the supervisor uses a state machine. It is driven by a pure function
that takes the current state and an event, and returns the next state alongside
instructions (like "spawn a thread" or "resolve a future").

Most of the time, you don't need to write a custom policy. You can just pass options
like `:backoff-fn`, `:max-attempts`, `:classify-error`, or `:default-throttle-ms` to
`create-supervisor`:

```clojure
(c/create-supervisor
  {:name        "llm"
   :max-attempts 5
   :backoff-fn  (fn [attempts] (* 250 attempts))          ; 250ms linear backoff
   :classify-error (fn [error]                            ; :fatal | :rate-limited | :transient
                     (cond
                       (= :auth (:type (ex-data error))) :fatal
                       :else :transient))})
```

`:classify-error` is the main extension point: return `:fatal` (abort + park now),
`:rate-limited` (throttle the whole supervisor), or `:transient` (retry with backoff).
This is how you trigger special behavior — e.g. classify an auth error as `:fatal` so
it parks immediately for you to refresh a token, or treat a specific status as
rate-limited. Per-task overrides go in the context map under the `:dj.concurrency/`
namespace (e.g. `:dj.concurrency/max-attempts`).

If you *do* need total control, write your own state transition function and pass it
as `:policy`. Read `default-policy` and `make-reference-policy` (re-exported from
`dj.concurrency`, implemented in [`src/dj/concurrency/policy.clj`](src/dj/concurrency/policy.clj))
to see how the default state machine handles success, failure, REPL interventions,
and shutdown. A policy is `(fn [event state] -> {:directives [...] :state new-state})`;
keep it pure — all side effects happen in the shell via the directives it returns.

---

## API reference

**Setup & execution**
- `create-supervisor` `[opts]` — `opts` keys: `:policy`, `:event-tap`, `:name`,
  `:store`, plus reference-policy opts when `:policy` is omitted (`:backoff-fn`,
  `:max-attempts`, `:classify-error`, `:default-throttle-ms`). Returns a supervisor
  map (plays nicely with Component/Integrant).
- `submit` `[sup context function]` — returns a future (supports `@`, 3-arg `deref`
  timeouts, and `realized?`). Put `:dj.concurrency/durable-key` in `context` to
  memoize the result; `:dj.concurrency/max-attempts` to override the retry budget.
- `stop!` `[sup]` / `[sup mode]` — stops the supervisor (`mode`: `:abort-pending`
  (default) or `:drop`).
- `wait-for-shutdown` `[sup]` — returns a promise that resolves when everything is
  fully stopped.

**Inspection (REPL / reconcile loop)**
- `state` `[sup]` — the whole pure state map.
- `tasks` `[sup]` — `{task-id task-map}` for all tracked tasks.
- `task` `[f]` / `[sup task-id]` — one task's map (status, context, error, …).
- `parked-tasks` `[sup]` — `{task-id task-map}` restricted to `:parked`.
- `tasks-by-status` `[sup]` — `{status [task-map ...]}`, the one-call lens for a poll
  loop (a filter, not a verdict).
- `explain-stuck` `[sup]` — `{:parked-count n :tasks [{:task-id :attempts :error
  :error-type :durable-key :age-ms} …]}`, an opinionated, pretty-printable summary of
  the parked set (oldest-first, all plain values). The level-triggered read; pairs with
  the edge-triggered `:event-tap`.

**Intervention (REPL / co-supervisor)** — each takes `[f]` or `[sup task-id]`:
- `retry` — re-run a parked/waiting/queued task with a fresh attempt budget.
- `deliver-result` `[… result]` — resolve the future with a supplied (possibly mock)
  value; never persisted to the store.
- `abort` `[… error]` — force the task to fail with a given `Throwable`.
- `cancel` — drop a non-running task (stops tracking its future).
- `clear-throttle` `[sup]` — lift a 429 throttle window immediately.
- `prune` `[sup]` / `[sup statuses]` — drop terminal tasks to reclaim memory.
- `evict!` `[sup key]` — remove a durable memo entry (for a stale cached result).

**Durable memoization (`dj.concurrency.store`)**
- `ResultStore` protocol — `lookup`, `record!`, `evict!`. Implement it over any
  journal.
- `atom-store` — non-durable in-memory store (tests / process memoization).
- Keys are plain EDN, stored verbatim (no helper needed) — e.g. `[:summarize prompt]`.

**Logging**
- `default-event-tap` `[entry]` — the loud dev-default tap (prints to `*err*`).

---

## Developing on this repo

```bash
nix develop            # clojure + babashka + jdk 21
clojure -X:test        # run the test suite
clojure -M:repl        # dev REPL (adds dev/ and test/ to the path)
```

The test suite has two layers: **pure policy tests** drive `default-policy` directly
with an explicit `:now` (deterministic, no threads), and **integration tests**
exercise a live supervisor with real virtual threads for the happy path and the
REPL-driven recovery workflow.

There's also a runnable interactive demo:

```bash
clojure -M:repl -e "(require 'dj.concurrency.llm-demo)(dj.concurrency.llm-demo/-main)"
```
*(Source: [`dev/dj/concurrency/llm_demo.clj`](dev/dj/concurrency/llm_demo.clj))*
