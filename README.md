# dj.concurrency

Richer concurrency primitives for Clojure — **manageable futures** — that make
unreliable, asynchronous work easy to drive and recover from, especially at the
REPL.

> Status: early. The core (`dj.concurrency`) is working and tested; the API may
> still change. Zero runtime dependencies.

## The problem

Imagine calling an LLM inference endpoint (or any flaky external service) from
the REPL. The *happy path* is trivial — send a prompt, get a completion — but in
practice it gets buried:

- the endpoint **transiently fails** (503s, dropped connections) → you add a
  retry loop,
- it **rate-limits you** (429s) → you add backoff + throttling, ideally shared
  across *all* in-flight requests,
- something genuinely breaks → your `try/catch` swallows the context, your REPL
  session unwinds, and you've lost the in-flight work you wanted to inspect.

The retry/throttle/error-handling plumbing ends up tangled into the business
logic, and a failure deep in a long chain throws away everything around it. It's
hectic.

## The approach: manageable futures

`dj.concurrency` splits async work into three layers:

1. a **consumer** that just `deref`s a promise and blocks (your happy-path code),
2. an async **worker** (a virtual thread) that runs your closure,
3. a central **supervisor** that owns the lifecycle: retries, throttling,
   parking, and recovery.

You hand the supervisor a *pure happy-path closure* plus a *context map*, and get
back a **promise stub** you `deref`. When something fails, the supervisor applies
policy (retry/backoff/throttle) entirely outside your business logic. When it
can't recover, it **parks** the task — leaving your consumer safely blocked,
*not* crashed — so you can inspect the context, fix the world, and either retry
or deliver a (possibly mocked) result. The blocked consumer then resumes as if
nothing happened.

## Requirements

- **JDK 21+** — uses virtual threads (`Thread/startVirtualThread`).
- **Clojure 1.11+** — uses `random-uuid`.

The library declares **no dependencies** (`:deps {}`), so your project chooses
its Clojure version. Dev tooling (a Nix flake + a test runner) is provided for
working *on* this repo.

## Use it in your project (deps.edn)

```clojure
io.github.bmillare/dj.concurrency {:git/sha "<sha>"}
```

Then:

```clojure
(require '[dj.concurrency :as c])
```

## Usage

### 1. The happy path stays trivial

```clojure
(def sup (c/create-supervisor {:name "llm"}))

;; Hand the supervisor a context map + a pure closure; get a stub back.
(def f (c/submit sup
                 {:prompt "summarize the meeting"}
                 (fn [] (call-llm "summarize the meeting"))))

;; The entire consumer. No retry loop, no try/catch.
(str "Report: " @f)
;;=> "Report: <completion>"
```

### 2. Transient failures retry automatically

If the closure throws a (non-fatal) error, the supervisor retries it with
exponential backoff — your `deref` just takes a little longer:

```clojure
;; closure fails twice with a 503, then succeeds
@(c/submit sup {:prompt "extract action items"} flaky-call)
;;=> "<completion>"   ; after 2 automatic retries
```

By default the reference policy classifies errors as:

- `:fatal` — `ex-data` has `{:type :business-error}` → abort immediately (no retry),
- `:rate-limited` — `ex-data` has `{:status 429}` → supervisor-wide throttle,
- `:transient` — anything else → retry up to `:mf/max-attempts` (default 3),
  then **park**.

Tune per-task via the context map, e.g. `{:mf/max-attempts 5}`, or set
supervisor-wide defaults when you create it:

```clojure
(c/create-supervisor {:name        "llm"
                      :max-attempts 5
                      :backoff-fn  (fn [attempts] (* 250 attempts))}) ; linear, 250ms steps
```

### 3. Rate limits throttle the *whole* supervisor

A `429` (with optional `:retry-after` ms in `ex-data`) opens a throttle window on
the supervisor. Sibling requests submitted during the window **queue** instead of
hammering the endpoint, then drain automatically when it lifts:

```clojure
;; req-1 gets a 429; req-2/req-3 submitted during the window queue up
(mapv deref [req-1 req-2 req-3])  ; all resolve once the throttle clears
;; you can also lift it manually:  (c/clear-throttle sup)
```

### 4. The payoff: park, inspect, recover — from the REPL

When retries are exhausted the task **parks**. The consumer stays blocked (its
context is preserved), and you debug live:

```clojure
(c/parked-tasks sup)            ; what's stuck?
(c/task f)                      ; full task map: status, context, the error
(:context (c/task f))           ;=> {:prompt "draft the release notes", :mf/attempts 1}
(ex-message (:error (c/task f)))

;; Then choose a recovery, without unwinding the blocked consumer:
(c/deliver-result f "MOCK: ...")  ; hand back a (mocked) result -> consumer unblocks
(c/retry f)                       ; or re-run the closure after hot-reloading code
(c/abort f (ex-info "give up" {})); or fail it deliberately
(c/cancel f)                      ; or drop it (consumer relies on its deref timeout)
```

A runnable, end-to-end version of all four scenarios lives in
[`dev/dj/concurrency/llm_demo.clj`](dev/dj/concurrency/llm_demo.clj):

```bash
clojure -M:repl -e "(require 'dj.concurrency.llm-demo)(dj.concurrency.llm-demo/-main)"
```

## API at a glance

Lifecycle / submission:

- `create-supervisor` `{:policy :log-fn :name :backoff-fn :max-attempts ...}` →
  supervisor (Component/Integrant-friendly map)
- `submit` `[sup context closure]` → promise stub (`deref` / 3-arg `deref` timeout / `realized?`)
- `stop!` `[sup]` / `[sup mode]` — `:abort-pending` (default) or `:drop`
- `wait-for-shutdown` `[sup]` → a promise realized once the shell stops:
  `(stop! sup)` then `@(wait-for-shutdown sup)`

Inspection (REPL):

- `state`, `tasks`, `task`, `parked-tasks`

Intervention (REPL):

- `deliver-result`, `retry`, `abort`, `cancel`, `clear-throttle`

## Logging (no dependency)

The library never pulls in a logging framework. Every internal event is handed
to a `:log-fn` you can supply to `create-supervisor`. The default is Clojure's
built-in **`tap>`** — a no-op unless you register a tap, so nothing is printed
and nothing conflicts with your app's logging:

```clojure
(add-tap (fn [entry] (println "[mf]" entry)))    ; opt in to seeing events
;; or route them yourself:
(c/create-supervisor {:log-fn (fn [entry] (my-logger/info entry))})
```

Each entry is a map like `{:level :debug :event :submit-executed :data <task-id>}`.

## Customizing policy

The supervisor is driven by a **pure** policy function
`(policy event state) -> {:directives [...] :state new-state}`. Events and
directives are positional `[type payload]` pairs; everything else is a map. The
shell executes the directives (spawn worker, resolve/abort a future, log) and
keeps `state` pure.

Most tuning needs no custom policy — pass `:backoff-fn`, `:max-attempts`,
`:classify-error`, or `:default-throttle-ms` to `create-supervisor` and they
thread into `make-reference-policy`. For full control, build a policy explicitly
with `(make-reference-policy opts)` or supply any `:policy` fn. See
`default-policy` / `make-reference-policy` in
[`src/dj/concurrency.clj`](src/dj/concurrency.clj) for the reference state
machine (submit / success / failed / tick / REPL events / shutdown).

## Developing on this repo

```bash
nix develop            # clojure + babashka + jdk 21
clojure -X:test        # run the test suite
clojure -M:repl        # dev REPL (adds dev/ and test/ to the path)
```

## License

See [LICENSE](LICENSE).
