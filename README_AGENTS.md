# dj.concurrency — in-depth guide

The companion to [`README.md`](README.md). The main README is the *why* and the
happy path; this doc is the *how it works* and the *how to operate it* — the
detail for contributors, coding agents, and power users. It assumes you've read the
main README's [payoff](README.md#the-payoff) and basic [usage](README.md#usage).

Contents:
- [The intended workflow: develop at the REPL, fix forward](#the-intended-workflow-develop-at-the-repl-fix-forward)
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

## The intended workflow: develop at the REPL, fix forward

Everything in this library is arranged around one development loop. The main README
states the philosophy ([fix forward, finish on the first run](README.md#philosophy-fix-forward-finish-on-the-first-run));
here is how it plays out in practice, and how to choose between the recovery modes.

**The loop.**

1. Write the workflow in **direct style** — plain `@`/`deref`, as if nothing fails.
   No retry loops, no `try/catch` scaffolding, no failure branches.
2. **Run it at the REPL.** Successful work resolves; the first genuine failure
   *parks* its task (after exhausting retries) and the workflow pauses at the `@`,
   holding every prior success.
3. **Look.** `(explain-stuck sup)` for the one-line "what's parked and why";
   `(task f)` / `(parked-tasks sup)` for the full context + error. The default loud
   `:event-tap` has already printed the park to `*err*`, so you usually know before
   you even ask.
4. **Fix forward.** Correct the real cause — hot-reload the worker fn, fix a prompt,
   refresh a credential, `(evict! sup key)` a stale memo — then `(retry f)` (re-run
   the fixed fn) or `(deliver-result f v)` (inject a known-good value). The parked
   `@` resumes; the run continues from where it stopped.
5. Repeat until the run completes **once**. A workflow that has run to completion a
   single time this way is one you've already debugged end-to-end.

The REPL here is not an ops console bolted on after the fact — it's the instrument
you *develop with*. Parking is what makes this possible: a failure deep in a chain
doesn't unwind the stack and destroy the successful work around it, so the state you
need to diagnose (and the results you don't want to recompute) are still sitting
there when you arrive.

### Fix-forward vs. durable re-run — when each applies

Two recovery modes exist; they are not interchangeable.

| | Fix forward (primary) | Durable re-run (safety net) |
|---|---|---|
| **When** | the process is still alive | the process actually died |
| **Mechanism** | park → inspect → fix code → `retry`/`deliver-result` → resume | re-run the same workflow; `:store` hits resolve completed work instantly |
| **Cost** | zero recompute; the same run finishes | re-run from the top, skipping only what's memoized |
| **Role** | the dream: finish on the first run | insurance for a crash, not the expected path |

Back the supervisor with a [durable memo table](#durable-results--crash-recovery) so
that *if* the JVM dies you don't re-pay for 49 of 50 LLM calls — but the aim is never
to need it. Fix-forward keeps you on the first run; the store only matters once that
run's process is gone.

### Unattended runs: a co-supervisor is you, automated

The loop above assumes *you* are at the REPL to service a park. For a run that must
proceed with no one watching — a scheduled job, a batch crawl — you write a
**co-supervisor**: a small program that watches for parks and acts in your place. It
is a stand-in for the human, so build it as the dumbest thing that captures what
you'd actually do, and climb this ladder only as far as a real run forces you:

1. **Dumbest, and often enough — timeout, fail, log.** Give each task an expected
   deadline; if it parks (or a bounded `deref` times out), `abort` it and dump the
   `explain-stuck` summary to your log. The run finishes with a clear record of what
   failed instead of hanging forever. **Start here** — for many batch jobs this is
   the whole co-supervisor.
2. **Slightly smarter — react to progress.** Retry a park a bounded number of times,
   or only while the task is still making progress, and escalate (abort + notify)
   when it isn't. The [reference co-supervisor](#a-reference-co-supervisor-you-can-copy-dev)
   is this tier: a per-task retry budget with a pluggable `:on-exhausted`.
3. **Smartest — a human or agent at the REPL.** No automated policy beats a person
   (or coding agent) who can read the error and *fix the code*. A co-supervisor buys
   you unattended progress; it does not replace the fix-forward judgment of step 4
   above. So when something it can't handle parks, the co-sup's real job is to
   **surface it clearly** — a durable, legible record — so a human can fix forward
   later, on the next run.

Build correctness on the poll surface (`explain-stuck`, `tasks-by-status`), use the
`:event-tap` only for latency, and keep the co-sup's action policy *yours* — see
[Reacting to events](#reacting-to-events-co-supervision) for the mechanics and a
copy-paste implementation.

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

### Park vs. abort

Recover vs. give up — the distinction the whole workflow turns on:

- **`:parked` is the recoverable state.** A retryable error exhausted its attempts, so the
  task is *paused, inspectable, and resumable* — non-terminal, waiting for you (or a
  co-supervisor) to fix forward and `retry`. This is the state the [fix-forward
  loop](#the-intended-workflow-develop-at-the-repl-fix-forward) is built on.
- **`:aborted` is the give-up state.** Terminal: `@f` re-throws, and the task **cannot be
  retried** back to life. It's the escape hatch for a failure that's never worth fixing —
  or one your calling code deliberately wants to catch on `deref`.

A task reaches `:aborted` three ways, none of them a normal retry outcome:

1. **An `:abort` classification** — a worker throws the `{:dj.concurrency/abort true}`
   marker (build it with `(abort-error msg data)`) and the default classifier ends the
   task; or a custom `:classify-error` returns `:abort` (see [Customizing
   policy](#customizing-policy)).
2. **A REPL / co-supervisor `abort`** — you decide to give up on a parked task.
3. **`stop!` in the default `:abort-pending` mode** — shutdown aborts every non-terminal
   task, parked ones included (use `(stop! sup :drop)` to skip that).

A worker exception *by itself* never aborts: unless it's classified `:abort`, it retries
and then **parks**. So in a fix-forward run you almost always want failures to park — reach
for abort only when you truly mean "stop, this one is done."

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

> **Or let the supervisor do it — `:pool-caps` (shipped).** The bound above is a
> property of the *backend*, so the library can hold it for you, per resource, inside
> one supervisor. Declare per-pool caps and tag each submit with its pool:
>
> ```clojure
> (def sup (c/create-supervisor {:pool-caps {:llm-a 4, :llm-b 2}}))
>
> ;; the caller names the endpoint; the supervisor enforces its cap
> (c/submit sup {:dj.concurrency/pool :llm-a} #(call-llm-a …))
> (c/submit sup {:dj.concurrency/pool :llm-b} #(call-llm-b …))
>
> (c/set-pool-cap! sup :llm-a 8)   ; retune a bound live
> ```
>
> Each pool is admitted **and** 429-throttled independently — a saturated or
> rate-limited backend never stalls another. A permit == a `:running` slot, so a
> parked/backed-off task frees it automatically (no wedged permit to babysit, unlike
> the semaphore above). Untagged work uses an unbounded `:default` pool; a submit to a
> pool with no declared cap runs unbounded and warns once (`:unknown-pool`). Omit
> `:pool-caps` entirely and behavior is exactly as before. Caps live in supervisor
> state and are retunable at runtime via `set-pool-cap!`. The consumer-side semaphore
> above is still fine when you want the gate outside the supervisor; `:pool-caps` is
> the same bound, first-class and observable (`:admission-wait`/`:admission-granted`
> carry `:pool`).

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
| `:submit-queued` | `:debug` | `{:task-id :pool}` | ✓ | submit entered its pool's admission queue (the single chokepoint) |
| `:unknown-pool` | `:warn` | `{:task-id :pool}` | ✓ | first submit to a pool with no declared cap; runs unbounded (warned once) |
| `:retry-scheduled` | `:debug` | `{:task-id :attempt :max-attempts :wake-in-ms}` | ✓ | a transient failure will be retried after a backoff |
| `:retry-queued` | `:debug` | `{:task-id :pool}` | ✓ | a REPL/co-sup retry re-entered its pool's admission queue |
| `:throttle-wait` | `:info` | `{:task-id :pool :wake-in-ms}` | ✓ | a 429 paused **that pool** for `:wake-in-ms` (other pools run on) |
| `:parked` | `:info` | task-id | ✓ | retries exhausted; the task awaits intervention |
| `:cancelled` | `:info` | task-id | ✓ | task cancelled via `cancel` |
| `:admission-granted` | `:debug` | `{:task-id :pool :in-flight :cap}` | ✓ | (capped pool) a permit freed; task admitted to a worker |
| `:admission-wait` | `:info` | `{:task-id :pool :in-flight :cap}` | ✓ | (capped pool) no free permit; task waiting for admission |
| `:pool-cap-set` | `:info` | `{:pool :cap}` | — | a pool's concurrency bound was set/retuned via `set-pool-cap!` |
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

### A reference co-supervisor you can copy (`dev/`)

The snippet above is the *shape*; a complete, tested implementation lives at
[`dev/dj/concurrency/reference_co_supervisor.clj`](dev/dj/concurrency/reference_co_supervisor.clj).
It is deliberately **not public API** — a copy-paste-quality starting point you lift
into your own code and adapt. It reconciles off `explain-stuck`, keeps a per-task retry
**budget** with a pluggable `:on-exhausted` escalation, optionally scopes to a `:pool`
for per-pool recovery, and reaches into **zero** backend internals (it settle-detects
from task status alone).

```clojure
(require '[dj.concurrency.reference-co-supervisor :as co])

;; recommended: cap the pool, then a naive reconcile+retry loop
(def sup  (c/create-supervisor {:pool-caps {:llm 4}}))
(def stop (co/start-co-supervisor sup {:pool :llm :budget 3}))
;; … later …
(stop)
```

**The cap-or-serialize contract.** Recovery retries can themselves stampede a slow
backend — the thundering herd, reborn at the recovery layer. Something must pace them,
and *either one alone* suffices:

- **Cap the pool** (`:pool-caps`) and the co-sup can be **naive** (the default): a
  co-sup `retry` re-enters the same admission gate as every other attempt, so it can't
  out-run the cap. Pacing is one number — this is the recommended recipe.
- **Or serialize** (`:serialize? true`) for an **unbounded** pool — service one park at
  a time. Needed only when the pool has no cap.

Don't do both; serialization under a cap is redundant. This keeps *prevention* (the
cap) and *recovery* (the co-sup) separate on purpose: the cap bounds load up front, the
co-sup mops up genuine failures — it is not the thing that paces load.

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
   :classify-error (fn [error]                            ; :abort | :rate-limited | :transient
                     (cond
                       ;; systematic rule: any 4xx is permanently invalid -> abort
                       (<= 400 (:status (ex-data error)) 499) :abort
                       :else :transient))})
```

`:classify-error` is the main extension point: return `:transient` (retry with backoff,
then park), `:rate-limited` (throttle that pool and retry when the window lifts), or
`:abort` (**terminal** — no retry, no park; `@f` re-throws). Reach for `:abort` only as an
escape hatch: a failure that's never worth fixing forward, or one your calling code is
waiting to catch on `deref`. Anything you'd *fix and retry* — a stale token, a code bug, a
bad prompt — should **park** instead (the default `:transient` path), so you can fix
forward and `retry`; see [park vs. abort](#park-vs-abort).

For a *one-off* dead end you don't need a classifier at all: throw
`(c/abort-error msg data)` from the worker and the **default** classifier aborts on its
`:dj.concurrency/abort` marker. A custom classifier can honor that same marker via
`(abort-requested? error)`. Per-task overrides go
in the context map under the `:dj.concurrency/` namespace (e.g.
`:dj.concurrency/max-attempts`).

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
- `abort-error` `[msg]` / `[msg data]` — build an `ex-info` tagged with the
  `:dj.concurrency/abort` marker; throw it from a worker to abort the task (the escape
  hatch — the task ends terminally and `@f` re-throws it). See [park vs.
  abort](#park-vs-abort).
- `abort-requested?` `[error]` — true if `error` carries the abort marker; for custom
  `:classify-error` fns that want to honor the escape hatch.
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
REPL-driven recovery workflow. The `:test` alias also puts `dev/` on the classpath so
the suite can exercise the reference co-supervisor
([`dev/dj/concurrency/reference_co_supervisor.clj`](dev/dj/concurrency/reference_co_supervisor.clj),
tested in `test/dj/concurrency/reference_co_supervisor_test.clj`).

There's also a runnable interactive demo:

```bash
clojure -M:repl -e "(require 'dj.concurrency.llm-demo)(dj.concurrency.llm-demo/-main)"
```
*(Source: [`dev/dj/concurrency/llm_demo.clj`](dev/dj/concurrency/llm_demo.clj))*
