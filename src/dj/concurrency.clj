(ns dj.concurrency
  "User-facing API for dj.concurrency: create and drive a supervisor, submit
   work, and inspect/intervene from the REPL.

   The library is split into a functional core and an imperative shell:
     - `dj.concurrency.shell`  — the impure runtime (event queue, virtual
                                 threads, directive interpreter, the future handle)
     - `dj.concurrency.policy` — the pure reference policy (the state machine)
   This namespace is the thin facade over both; requiring it is all you need.

   Return-value style: every function here returns a MAP, except the two data
   shapes that are intrinsically positional pairs and travel through the
   queue/policy verbatim — events `[event-type payload]` and directives
   `[directive-type payload]`."
  (:require [clojure.core.protocols :as p]
            [clojure.pprint :as pp]
            [dj.concurrency.shell :as shell]
            [dj.concurrency.policy :as policy]
            [dj.concurrency.store :as store])
  (:import [java.util.concurrent LinkedBlockingQueue BlockingQueue CompletableFuture]
           [dj.concurrency.shell ManageableFuture]))

;; =============================================================================
;; User API
;; =============================================================================

(defn default-event-tap
  "The default `:event-tap`: a loud development breadcrumb.

   Prints every supervisor event to `*err*` as a compact one-liner the instant
   it is emitted, so a run never parks in total silence. (The pre-rename default
   was `tap>`, which is invisible unless you register a tap — a run could pile up
   retries or park with nothing printed.)

   This is deliberately unfiltered and human-oriented — a dev aid, not a
   production logging strategy. In production, pass your own `:event-tap` to
   route entries into your logger/telemetry, `tap>` for the old silent behavior,
   or a level filter of your own.

   Entry shape (the raw contract): `{:level kw :event kw :data any}`, plus a
   top-level `:task-id` on every task-scoped event (nil/absent for genuinely
   supervisor-scoped events like `:pruned`/`:unknown-event`). `(:task-id entry)`
   is the reliable handle to feed an intervention — see `create-supervisor`."
  [{:keys [level event data]}]
  (binding [*out* *err*]
    (println (format "dj.concurrency %-6s %-18s %s"
                     (or (some-> level name) "")
                     (or (some-> event name) "")
                     (pr-str data)))))

(defn create-supervisor
  "Creates and starts a new Manageable Futures supervisor.

   Options:
     :policy   - A pure policy `(event state) -> {:directives [...] :state s'}`.
                 Defaults to `(make-reference-policy opts)`.
     :event-tap - `(fn [entry-map])` invoked for every :log directive — the one
                 place a running supervisor's lifecycle transitions escape to
                 observers. Defaults to `default-event-tap` (a loud dev
                 breadcrumb to `*err*`). Pass `tap>` for the old silent
                 default, or your own fn to route into a logger.

                 CONTRACT: the tap is called SYNCHRONOUSLY, on the supervisor's
                 single control thread. It MUST be fast and non-blocking — do no
                 I/O, don't deref a task, don't acquire locks. A throw is
                 swallowed, but a BLOCK wedges the whole supervisor (the control
                 loop stops polling AND stops clearing throttle/admission
                 deadlines). To react (retry/abort/notify), hand the entry to
                 your own queue or virtual thread and act from there — the tap is
                 a notification sink, not the place to do work. Entries carry a
                 top-level `:task-id` for task-scoped events, so a reactor uses
                 `(:task-id entry)` as the handle. The tap is LOSSY/best-effort:
                 build correctness on a poll+reconcile loop (see `tasks-by-status`
                 / `parked-tasks`) and use the tap only to react sooner.
     :store    - Optional dj.concurrency.store/ResultStore. When present, tasks
                 whose context contains :dj.concurrency/durable-key are memoized:
                 a prior recorded result short-circuits execution and resolves
                 the future with the cached value (task is annotated :cached?).
                 Results are persisted durably BEFORE the future resolves.
     :name     - Optional name for logging/identification.

   Reference-policy options are also read from `opts` when :policy is not given
   (see `make-reference-policy`): :backoff-fn, :max-attempts, :classify-error,
   :default-throttle-ms.

   Returns a map representing the supervisor (queue, state atom, running shell
   thread, shutdown-promise). Easily integrated into Component/Integrant."
  [opts]
  (let [q      (LinkedBlockingQueue.)
        state  (atom {:tasks               {}
                      :throttle-expires-at nil
                      :shutdown?           false})
        policy (or (:policy opts) (policy/make-reference-policy opts))
        event-tap (:event-tap opts default-event-tap)
        sup    {:queue            q
                :state            state
                :policy           policy
                :event-tap        event-tap
                :store            (:store opts)        ;; optional ResultStore
                :name             (:name opts)
                :shutdown-promise (promise)}]
    ;; datafy of the supervisor returns its pure state map (not the raw
    ;; plumbing), so REBL/Portal/Morse show something useful. Metadata-based
    ;; protocol extension keeps the supervisor a plain map.
    (-> sup
        ;; `state` names the local atom in this let, so reach the pure map directly.
        (assoc :shell-thread (shell/run-shell! sup))
        (with-meta {`p/datafy (fn [s] @(:state s))}))))

(defn submit
  "Submits a closure to the supervisor for execution.
   Returns a ManageableFuture immediately.

   Throws if the supervisor is shutting down or already shut down."
  [supervisor context closure]
  ;; Design decision RI-12 §5: Stop-then-submit is a programming error.
  ;; Reject immediately at the call site rather than enqueueing.
  (if (:shutdown? @(:state supervisor))
    (throw (ex-info "Supervisor is stopped or shutting down" {:type ::shutdown}))

    (let [task-id (random-uuid)
          cf      (CompletableFuture.)
          stub    (shell/->ManageableFuture task-id context supervisor cf)]

      ;; Fire-and-forget
      (.put ^BlockingQueue (:queue supervisor)
            [:submit {:task-id      task-id
                      :context      context
                      :closure      closure
                      :submitted-at (System/currentTimeMillis)
                      :cf           cf}])
      stub)))

(defn stop!
  "Initiates supervisor shutdown.

   Modes:
     :abort-pending (default) - Instructs the policy to abort all non-terminal
                                tasks (throwing an ex-info in consumer threads).
     :drop                    - Silently ignores pending tasks. Blocked consumers
                                will rely on their own deref timeouts.

   Returns the supervisor. Shutdown is asynchronous; use `wait-for-shutdown` to
   block until the shell has fully stopped."
  ([supervisor]
   (stop! supervisor :abort-pending))
  ([supervisor mode]
   (.put ^BlockingQueue (:queue supervisor)
         [:shutdown {:mode mode}])
   supervisor))

(defn wait-for-shutdown
  "Returns the supervisor's shutdown promise, realized with `true` once the shell
   loop has fully terminated. Deref it to block until shutdown completes:

     (stop! sup)
     @(wait-for-shutdown sup)          ; blocks until stopped
     (deref (wait-for-shutdown sup) 1000 :timed-out)  ; with a timeout

   Already-stopped supervisors return an already-realized promise."
  [supervisor]
  (:shutdown-promise supervisor))

;; =============================================================================
;; REPL API
;; =============================================================================

;; --- Inspection ---

(defn state
  "Returns the complete, pure state map of the supervisor."
  [sup]
  @(:state sup))

(defn tasks
  "Returns all tasks currently tracked by the supervisor."
  [sup]
  (:tasks @(:state sup)))

(defn task
  "Returns the state of a specific task."
  ([stub]
   (task (:supervisor stub) (:task-id stub)))
  ([sup task-id]
   (get-in @(:state sup) [:tasks task-id])))

(defn parked-tasks
  "Returns a map of all parked tasks awaiting REPL intervention."
  [sup]
  (into {} (filter (fn [[_ t]] (= :parked (:status t))) (tasks sup))))

(defn tasks-by-status
  "Groups the supervisor's tasks by `:status`, returning
   `{status [task ...]}` (e.g. `:parked`, `:queued`, `:waiting-retry`,
   `:running`, `:resolved`, `:aborted`, `:cancelled`).

   The one-call lens for a co-supervisor's poll+reconcile loop: read it on your
   own cadence, decide per group, and act via the interventions (`retry`,
   `deliver-result`, `abort`, `cancel`, `prune`). This is authoritative and never
   misses — unlike the `:event-tap`, which is lossy and only improves latency. It
   is a filter, not a verdict: what each group means is your policy, not the
   library's."
  [sup]
  (group-by :status (vals (tasks sup))))

(defn explain-stuck
  "Returns an opinionated, pretty-printable summary of the supervisor's PARKED
   tasks — the authoritative \"needs a human/agent\" set that will otherwise block
   `deref` forever. The pull / level-triggered companion to the push /
   edge-triggered `:event-tap`: the tap tells you WHEN something parks (lossy);
   this shows you WHAT is parked right now (never lossy).

   Returns `{:parked-count n :tasks [task ...]}` where each task is a flat,
   plain-value map

     {:task-id :attempts :error <msg-string> :error-type :durable-key :age-ms}

   sorted oldest-first (largest `:age-ms`). Pure data: `pprint` it at a REPL, or
   filter/reconcile off `:tasks` in a co-supervisor, e.g.

     (->> (explain-stuck sup) :tasks (filter …) (run! #(retry sup (:task-id %))))

   It condenses the raw parked-task maps — flattening the `:error` Throwable to
   its message string, pulling attempts/durable-key out of internal `:context`
   key paths, and deriving age from `:submitted-at` — so no field values are
   Throwables or closures (unlike the raw `parked-tasks` map). The map return
   grows by adding keys, so callers that destructure `:tasks` stay stable.

   Scopes to `:parked` only — the set that needs intervention. `:waiting-retry`
   is self-healing and `:queued`/`:running` are progressing; for the fuller
   picture use `tasks-by-status`."
  [sup]
  (let [now   (System/currentTimeMillis)
        tasks (->> (parked-tasks sup)
                   (map (fn [[id t]]
                          (let [err (:error t)]
                            {:task-id     id
                             :attempts    (get-in t [:context :dj.concurrency/attempts])
                             :error       (some-> err ex-message)
                             :error-type  (:type (ex-data err))
                             :durable-key (get-in t [:context :dj.concurrency/durable-key])
                             :age-ms      (when-let [s (:submitted-at t)] (- now s))})))
                   ;; oldest-first: largest age reads first. `compare` is nil-safe
                   ;; (nil sorts last) in case a task ever lacks :submitted-at.
                   (sort-by :age-ms #(compare %2 %1))
                   vec)]
    {:parked-count (count tasks)
     :tasks        tasks}))

;; --- Printing + datafy/nav ---
;;
;; A ManageableFuture is a *handle*, not data: printing one must never dump the
;; whole supervisor map (state atom, every task, every closure). We print a
;; compact, deliberately-unreadable form and expose the real task map through
;; datafy for inspector tooling (Portal/Morse/REBL). The handle's runtime
;; behavior (blocking deref) lives with the record in `dj.concurrency.shell`;
;; this presentation lives here because it reaches into REPL state via `task`.

(defmethod print-method ManageableFuture
  [^ManageableFuture mf ^java.io.Writer w]
  (.write w (str "#<dj.concurrency/future "
                 (:task-id mf)
                 " :status " (pr-str (or (:status (task mf)) :submitted))
                 " :realized? " (.isDone ^CompletableFuture (:delegate mf))
                 ">")))

;; pprint uses its own dispatch table and would otherwise walk the record as a map.
(defmethod pp/simple-dispatch ManageableFuture
  [mf]
  (print-method mf *out*))

(extend-protocol p/Datafiable
  ManageableFuture
  (datafy [mf]
    ;; :submitted covers the window before the shell has processed the :submit.
    (let [t (or (task mf) {:task-id (:task-id mf) :status :submitted})]
      (with-meta
        (assoc t
               :realized?  (realized? mf)
               :supervisor (select-keys (:supervisor mf) [:name]))
        {`p/nav (fn [_coll k v]
                  ;; navigating :supervisor drills into the full supervisor state
                  (if (= k :supervisor)
                    (state (:supervisor mf))
                    v))}))))


;; --- Interventions ---

(defn retry
  "Requests immediate re-execution of a parked, waiting, or queued task.

   Grants a FRESH attempt budget: the task's `:dj.concurrency/attempts` counter
   is reset to 1, so it gets the full `:max-attempts` again. Rationale: a REPL
   `retry` means \"I changed something, give it a clean run\"."
  ([stub]
   (retry (:supervisor stub) (:task-id stub)))
  ([sup task-id]
   (.put ^BlockingQueue (:queue sup) [:repl/retry {:task-id task-id}])
   :queued))

(defn deliver-result
  "Manually supplies a (possibly mocked) result for a task.
   The policy will instruct the shell to resolve the CompletableFuture."
  ([stub result]
   (deliver-result (:supervisor stub) (:task-id stub) result))
  ([sup task-id result]
   (.put ^BlockingQueue (:queue sup) [:repl/deliver {:task-id task-id :result result}])
   :queued))

(defn abort
  "Forces the task to fail with a specific Throwable.
   The policy will instruct the shell to abort the CompletableFuture."
  ([stub error]
   (abort (:supervisor stub) (:task-id stub) error))
  ([sup task-id error]
   (.put ^BlockingQueue (:queue sup) [:repl/abort {:task-id task-id :error error}])
   :queued))

(defn cancel
  "Discards a task without completing its CompletableFuture.
   The consumer thread remains blocked until its own deref timeout occurs.

   The task's CF is dropped from the supervisor's internal tracking (so it is not
   leaked), but it is never completed — consumer semantics are unchanged: a bare
   `@f` blocks forever; use `(deref f timeout-ms timeout-val)`."
  ([stub]
   (cancel (:supervisor stub) (:task-id stub)))
  ([sup task-id]
   (.put ^BlockingQueue (:queue sup) [:repl/cancel {:task-id task-id}])
   :queued))

(defn prune
  "Removes terminal tasks (:resolved, :aborted, :cancelled) from supervisor
   state, releasing their contexts, closures, and errors.

   Optionally pass a set of statuses to restrict pruning, e.g.
   (prune sup #{:resolved}). Non-terminal statuses in the set are ignored —
   parked/waiting/running tasks may have blocked consumers and cannot be
   pruned. Compose your own GC policy on top of this (e.g. call it on a
   schedule, or after inspecting `tasks`).

   Fire-and-forget; returns :queued."
  ([sup] (prune sup policy/terminal-statuses))
  ([sup statuses]
   (.put ^BlockingQueue (:queue sup) [:repl/prune {:statuses (set statuses)}])
   :queued))

(defn clear-throttle
  "Manually lifts a supervisor-wide throttle window, draining the deferred queue."
  [sup]
  (.put ^BlockingQueue (:queue sup) [:repl/clear-throttle {}])
  :queued)

(defn evict!
  "Removes a durable-key's entry from the supervisor's result store, so the
   next task submitted with that key re-executes instead of pulling the memo.
   No-op (returns nil) if the supervisor has no store.

   This acts on the store directly (durably); it does not queue an event and
   does not touch task state or any in-flight task. Use it before `retry` when
   a cached value has gone stale (e.g. the prompt template changed)."
  [sup k]
  (when-let [s (:store sup)]
    (store/evict! s k)
    :evicted))

;; =============================================================================
;; Reference policy (re-exports)
;; =============================================================================
;; The reference policy lives in `dj.concurrency.policy`. These re-exports keep
;; the public surface in one namespace: most users configure it through
;; `create-supervisor` opts, but you can also build/customize a policy directly.

(def ^{:arglists '([opts])} make-reference-policy
  "Builds a pure reference policy `(fn [event state] -> {:directives :state})`,
   closing over `opts`. Recognized opts (others ignored):
     :classify-error      (fn [error] -> :fatal | :rate-limited | :transient)
     :backoff-fn          (fn [attempts] -> backoff-ms) for transient retries
     :max-attempts        default max attempts when a task's context omits
                          :dj.concurrency/max-attempts (default 3)
     :default-throttle-ms throttle window for a 429 with no :retry-after (ms)

   Pass the result as `:policy` to `create-supervisor` for total control, or
   pass the individual opts to `create-supervisor` to tweak the default.
   See `dj.concurrency.policy` for the full state machine."
  policy/make-reference-policy)

(def default-policy
  "The reference policy built with all defaults; equals `(make-reference-policy {})`."
  policy/default-policy)
