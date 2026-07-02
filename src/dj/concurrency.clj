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
            [dj.concurrency.policy :as policy])
  (:import [java.util.concurrent LinkedBlockingQueue BlockingQueue CompletableFuture]
           [dj.concurrency.shell ManageableFuture]))

;; =============================================================================
;; User API
;; =============================================================================

(defn create-supervisor
  "Creates and starts a new Manageable Futures supervisor.

   Options:
     :policy   - A pure policy `(event state) -> {:directives [...] :state s'}`.
                 Defaults to `(make-reference-policy opts)`.
     :log-fn   - `(fn [entry-map])` invoked for every :log directive. Defaults to
                 `tap>` (dependency-free; silent unless you register a tap).
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
        log-fn (:log-fn opts tap>)
        sup    {:queue            q
                :state            state
                :policy           policy
                :log-fn           log-fn
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
