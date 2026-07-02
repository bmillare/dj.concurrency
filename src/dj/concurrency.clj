(ns dj.concurrency
  (:require [clojure.core.protocols :as p]
            [clojure.pprint :as pp])
  (:import [java.util.concurrent BlockingQueue LinkedBlockingQueue CompletableFuture
            TimeUnit TimeoutException ExecutionException]
           [clojure.lang IDeref IBlockingDeref IPending]))

;; Return-value style: every function here returns a MAP, except the two
;; data shapes that are intrinsically positional pairs and travel through the
;; queue/policy verbatim:
;;   - events     [event-type   payload-map]
;;   - directives  [directive-type payload-map]
;; Everything else (policy results, internal shell bindings) uses maps so the
;; code reads by key rather than by position.

(defrecord ManageableFuture [task-id context supervisor ^CompletableFuture delegate]
  IDeref
  (deref [_]
    (try
      (.get delegate)
      (catch ExecutionException e
        ;; CompletableFuture wraps exceptions in an ExecutionException.
        ;; We unwrap it here so that the blocked consumer receives the real
        ;; throwable (e.g., the ex-info from a supervisor :abort directive).
        (throw (or (.getCause e) e)))))

  IBlockingDeref
  (deref [_ timeout-ms timeout-val]
    (try
      (.get delegate timeout-ms TimeUnit/MILLISECONDS)
      (catch TimeoutException _
        timeout-val)
      (catch ExecutionException e
        (throw (or (.getCause e) e)))))

  IPending
  (isRealized [_]
    (.isDone delegate)))

(defn- ms-until-next-deadline
  "Scans the pure state for the soonest upcoming deadline.
   Returns milliseconds until that deadline, or Long/MAX_VALUE if none."
  [state now]
  ;; "at"s are unix timestamps (ts) since epoch
  (let [wake-ats    (->> (:tasks state)
                         vals
                         ;; Only tasks actually waiting on a deadline count. A
                         ;; terminal task that kept a stale :wake-at (from before
                         ;; a REPL intervention) must NOT drive the poll timeout
                         ;; to 0 forever -> 100% CPU spin.
                         (filter #(= :waiting-retry (:status %)))
                         (keep :wake-at)) ;; seq of ts
        throttle-at (:throttle-expires-at state) ;; ts or nil
        deadlines   (if throttle-at
                      (conj wake-ats throttle-at)
                      wake-ats)]
    (if (seq deadlines)
      (max 0 (- (apply min deadlines) now))
      Long/MAX_VALUE)))

(defn- safe-log!
  "Invokes the supervisor's :log-fn, swallowing anything it throws.
   A broken logger must never take down the shell loop."
  [sup entry]
  (try ((:log-fn sup) entry)
       (catch Throwable _ nil)))

(defn- drain-stranded-submits!
  "Completes exceptionally the CF of any :submit still sitting on the queue.
   Called on shutdown so a submit that raced past `submit`'s shutdown check
   (check-then-put) doesn't leave its consumer blocked on a dead queue forever."
  [^BlockingQueue queue]
  (loop []
    (when-let [[etype payload] (.poll queue)]
      (when (= :submit etype)
        (when-let [^CompletableFuture cf (:cf payload)]
          (.completeExceptionally
           cf (ex-info "supervisor stopped" {:type ::shutdown}))))
      (recur))))

(defn- execute-worker-wrapper
  "Code that runs on a VirtualThread (in execute-directive!). Runs the
  user's closure and routes the result back to the supervisor's single
  event queue."
  [{:keys [^BlockingQueue queue]} {:keys [task-id context closure]}]
  (let [start (System/currentTimeMillis)]
    (try
      (let [result (closure)]
        (.put queue [:success {:task-id  task-id
                               :result   result
                               :duration (- (System/currentTimeMillis) start)}]))
      (catch Throwable t
        (.put queue [:failed {:task-id  task-id
                              :context  context
                              :error    t
                              :duration (- (System/currentTimeMillis) start)}])))))

;; Default Directive Interpreter
;; - Could convert this to multimethod version if needed
(defn- execute-directive!
  "Executes a single side-effecting directive.
   Returns the updated CF registry (removing CFs when tasks reach a terminal state)."
  [sup cf-registry [dir-type payload]]
  (case dir-type
    :execute
    (do
      (Thread/startVirtualThread #(execute-worker-wrapper sup payload))
      cf-registry)

    :resolve
    (let [cf ^CompletableFuture (get cf-registry (:task-id payload))]
      (when cf
        (.complete cf (:result payload)))
      ;; Enforce single-completion at the shell level and prevent memory leaks
      (dissoc cf-registry (:task-id payload)))

    :abort
    (let [cf ^CompletableFuture (get cf-registry (:task-id payload))]
      (when cf
        (.completeExceptionally cf (:error payload)))
      (dissoc cf-registry (:task-id payload)))

    :drop-cf
    ;; Stop tracking a task's CF without completing it (cancel semantics; also a
    ;; defensive sweep on prune), so the registry doesn't leak CFs the shell will
    ;; never resolve/abort.
    (dissoc cf-registry (:task-id payload))

    :log
    (do
      ;; Pluggable, dependency-free logging: hand the raw entry map to the
      ;; supervisor's :log-fn. Default is `tap>` (clojure.core) which is a no-op
      ;; unless the user registers a tap, so we never depend on or fight with a
      ;; logging system. Override via {:log-fn ...} on create-supervisor.
      (safe-log! sup payload)
      cf-registry)))

;; Supervisor main loop
(defn run-shell!
  "Starts the supervisor's single-threaded event loop on a Virtual Thread.
   `sup` must contain: {:queue BlockingQueue, :state Atom, :policy IFn,
   :log-fn IFn, :shutdown-promise promise}.

   The policy is called as `(policy event state)` and must return a map
   `{:directives [...] :state new-state}`."
  [{:keys [^BlockingQueue queue state policy shutdown-promise] :as sup}]
  (Thread/startVirtualThread
   (fn []
     (try
       (loop [cf-registry {}] ;; task-id -> CF: Track CFs out of state to keep state pure
         (let [{next-registry :registry shutdown? :shutdown?}
               ;; The whole iteration (steps 1-6) is guarded: an unexpected throw
               ;; must NOT kill the shell. Only a :shutdown? state (or the thread
               ;; dying) ends the loop — a dead shell is otherwise indistinguishable
               ;; from a clean shutdown.
               (try
                 (let [;; 1. Get event
                       poll-now  (System/currentTimeMillis)
                       timeout   (ms-until-next-deadline @state poll-now)
                       raw-event (.poll queue timeout TimeUnit/MILLISECONDS) ;; wait for event or timeout
                       ;; Re-stamp the clock AFTER the (possibly long) poll so a
                       ;; deadline-fired tick carries the real current time, not a
                       ;; timestamp captured before the wait.
                       now       (System/currentTimeMillis)
                       event     (if raw-event
                                   [(first raw-event) (assoc (second raw-event) :now now)] ;; Time is plumbed in
                                   [:tick {:now now}])

                       ;; 2. Handle/Extract CF (keep the live CF out of the pure policy)
                       {clean-event :event next-cf-registry :cf-registry}
                       (if (= :submit (first event))
                         (let [payload       (second event)
                               cf            (:cf payload)
                               clean-payload (dissoc payload :cf)]
                           {:event       [:submit clean-payload]
                            :cf-registry (if cf
                                           (assoc cf-registry (:task-id payload) cf)
                                           cf-registry)})
                         {:event event :cf-registry cf-registry})

                       ;; 3. Get directives + state transition from policy
                       {:keys [directives] state' :state}
                       (try
                         (policy clean-event @state)
                         (catch Throwable t ;; Fallback: empty directives and log on policy throw
                           (safe-log! sup {:level :error :event :policy-threw
                                           :data {:event clean-event :error t}})
                           {:directives [] :state @state}))]

                   ;; 4. Commit state
                   (reset! state state')

                   ;; 5. Execute directives sequentially
                   (let [final-registry (reduce (fn [reg dir]
                                                  (execute-directive! sup reg dir))
                                                next-cf-registry
                                                directives)]
                     {:registry final-registry :shutdown? (:shutdown? state')}))
                 (catch Throwable t
                   (safe-log! sup {:level :error :event :shell-iteration-threw
                                   :data {:error t}})
                   ;; Continue with the pre-iteration registry.
                   {:registry cf-registry :shutdown? false}))]

           ;; 6. Recur or terminate
           (when-not shutdown?
             (recur next-registry))))
       (finally
         ;; Complete any :submit that raced past the shutdown check so its
         ;; consumer doesn't block on a dead queue forever.
         (drain-stranded-submits! queue)
         ;; Realize the shutdown promise on ANY loop exit (clean or abnormal)
         ;; so `wait-for-shutdown` always unblocks.
         (deliver shutdown-promise true))))))


;; =============================================================================
;; User API
;; =============================================================================

(declare make-reference-policy)

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
        policy (or (:policy opts) (make-reference-policy opts))
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
        (assoc :shell-thread (run-shell! sup))
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
          stub    (->ManageableFuture task-id context supervisor cf)]

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

;; The three statuses a task can never leave: safe to prune, safe to drop the CF.
(def ^:private terminal-statuses #{:resolved :aborted :cancelled})

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
;; datafy for inspector tooling (Portal/Morse/REBL).

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
  ([sup] (prune sup terminal-statuses))
  ([sup statuses]
   (.put ^BlockingQueue (:queue sup) [:repl/prune {:statuses (set statuses)}])
   :queued))

(defn clear-throttle
  "Manually lifts a supervisor-wide throttle window, draining the deferred queue."
  [sup]
  (.put ^BlockingQueue (:queue sup) [:repl/clear-throttle {}])
  :queued)

;; =============================================================================
;; Reference Policy
;; =============================================================================

(defn default-classify-error
  "Reference heuristic for classifying errors based on the dispatch table examples.
   In a production system, supply your own via `make-reference-policy`."
  [error]
  (let [d (ex-data error)]
    (cond
      (= (:type d) :business-error) :fatal
      (= (:status d) 429)           :rate-limited
      :else                         :transient)))

(defn default-backoff
  "Simple exponential backoff (1s, 2s, 4s...) based on attempts."
  [attempts]
  (long (* 1000 (Math/pow 2 (dec attempts)))))

(def default-reference-opts
  "Defaults for the reference policy. Override any via `make-reference-policy`
   (or by passing them to `create-supervisor`)."
  {:classify-error      default-classify-error
   :backoff-fn          default-backoff
   :max-attempts        3
   :default-throttle-ms 5000})

;; Event transition logic
(defn- apply-event
  "Evaluates a single event against the current state, using `config` for the
   tunable bits (error classification, backoff, attempts, throttle window).
   Returns a map {:directives [...] :state {...}}."
  [config [event-type payload] state now]
  (let [task-id (:task-id payload)
        t       (get-in state [:tasks task-id])
        status  (:status t)]

    (case event-type

      ;; --- S: Submit ---
      :submit
      (if (:shutdown? state)
        ;; S1: Supervisor stopped
        {:directives [[:abort {:task-id task-id
                               :error (ex-info "supervisor stopped" {:type ::shutdown})}]]
         :state state}

        (let [throttled? (and (:throttle-expires-at state)
                              (< now (:throttle-expires-at state)))]
          (if throttled?
            ;; S2: Throttled
            {:directives [[:log {:level :info :event :submit-throttled :data task-id}]]
             :state (assoc-in state [:tasks task-id]
                              {:task-id task-id, :status :queued
                               ;; Seed ::attempts like S3 so a later failure never
                               ;; increments a missing counter.
                               :context (assoc (:context payload) ::attempts 1)
                               :closure (:closure payload)
                               :submitted-at (:submitted-at payload)})}
            ;; S3: Normal Submit
            (let [ctx' (assoc (:context payload) ::attempts 1)]
              {:directives [[:execute {:task-id task-id :context ctx' :closure (:closure payload)}]
                            [:log {:level :debug :event :submit-executed :data task-id}]]
               :state (assoc-in state [:tasks task-id]
                                {:task-id task-id, :status :running
                                 :context ctx', :closure (:closure payload)
                                 :submitted-at (:submitted-at payload)})}))))

      ;; --- K: Success ---
      :success
      (cond
        ;; K1: Late/Ignored
        (or (nil? t) (terminal-statuses status))
        {:directives [[:log {:level :warn :event :late-success :data task-id}]] :state state}

        ;; K2: Expected Success
        (= :running status)
        {:directives [[:resolve {:task-id task-id :result (:result payload)}]]
         :state (assoc-in state [:tasks task-id :status] :resolved)}

        ;; K3: Illegal
        :else
        {:directives [[:log {:level :warn :event :illegal-transition :data task-id}]] :state state})

      ;; --- F: Failed ---
      :failed
      (cond
        ;; F1: Late/Ignored
        (or (nil? t) (terminal-statuses status))
        {:directives [[:log {:level :warn :event :late-failure :data task-id}]] :state state}

        (= :running status)
        (let [err-type     ((:classify-error config) (:error payload))
              attempts     (get-in t [:context ::attempts] 1)
              max-attempts (get-in t [:context ::max-attempts] (:max-attempts config))]
          (case err-type
            ;; F2: Fatal
            :fatal
            {:directives [[:abort {:task-id task-id :error (:error payload)}]]
             :state (update-in state [:tasks task-id] assoc :status :aborted :error (:error payload))}

            ;; F3: Rate Limited (429)
            :rate-limited
            (let [window  (or (:retry-after (ex-data (:error payload))) (:default-throttle-ms config))
                  wake-at (+ now window)]
              {:directives []
               ;; A concurrent, shorter 429 must not shorten a live window, so
               ;; keep the later expiry. A 429 doesn't consume the retry budget
               ;; (being throttled isn't evidence the task is broken). :throttle?
               ;; tags the waiter so clear-throttle can wake exactly these tasks.
               :state (-> state
                          (update :throttle-expires-at (fnil max 0) wake-at)
                          (update-in [:tasks task-id] assoc
                                     :status :waiting-retry :wake-at wake-at
                                     :throttle? true :error (:error payload)))})

            ;; F4 & F5: Transient
            :transient
            (if (< attempts max-attempts)
              ;; F4: Retryable
              (let [backoff ((:backoff-fn config) attempts)]
                {:directives []
                 :state (-> state
                            (update-in [:tasks task-id] assoc
                                       :status :waiting-retry :wake-at (+ now backoff) :error (:error payload))
                            (update-in [:tasks task-id :context ::attempts] (fnil inc 1)))})

              ;; F5: Exhausted / Parked
              {:directives [[:log {:level :info :event :parked :data task-id}]]
               :state (update-in state [:tasks task-id] assoc :status :parked :error (:error payload))})))

        ;; F6: Illegal
        :else
        {:directives [[:log {:level :warn :event :illegal-transition :data task-id}]] :state state})

      ;; --- T: Tick ---
      :tick
      {:directives [] :state state} ;; Handled purely by the deadline-scan post-step

      ;; --- R: REPL Retry ---
      :repl/retry
      (cond
        ;; R1 — a REPL retry grants a fresh attempt budget ("I changed something").
        (#{:parked :waiting-retry :queued} status)
        (let [t' (assoc-in t [:context ::attempts] 1)]
          {:directives [[:execute (select-keys t' [:task-id :context :closure])]]
           :state (update-in state [:tasks task-id] assoc
                             :status :running :wake-at nil :throttle? nil
                             :context (:context t'))})
        ;; R2 & R3
        (= :running status)
        {:directives [[:log {:level :warn :event :already-running :data task-id}]] :state state}
        :else
        {:directives [[:log {:level :warn :event :no-such-task :data task-id}]] :state state})

      ;; --- D: REPL Deliver ---
      ;; Deliver/abort/cancel all clear :wake-at: a terminal task must never leave
      ;; a live deadline behind, or the deadline scan spins on it.
      :repl/deliver
      (if (#{:parked :waiting-retry :queued :running} status)
        {:directives [[:resolve {:task-id task-id :result (:result payload)}]]
         :state (update-in state [:tasks task-id] assoc :status :resolved :wake-at nil)}
        {:directives [[:log {:level :warn :event :invalid-deliver :data task-id}]] :state state})

      ;; --- A: REPL Abort ---
      :repl/abort
      (if (#{:parked :waiting-retry :queued :running} status)
        {:directives [[:abort {:task-id task-id :error (:error payload)}]]
         :state (update-in state [:tasks task-id] assoc :status :aborted :wake-at nil)}
        {:directives [[:log {:level :warn :event :invalid-abort :data task-id}]] :state state})

      ;; --- C: REPL Cancel ---
      :repl/cancel
      (cond
        ;; C1 — :drop-cf stops the shell tracking the never-completed CF;
        ;; consumer semantics are unchanged (see `cancel`).
        (#{:parked :waiting-retry :queued} status)
        {:directives [[:drop-cf {:task-id task-id}]
                      [:log {:level :info :event :cancelled :data task-id}]]
         :state (update-in state [:tasks task-id] assoc :status :cancelled :wake-at nil)}
        ;; C2 & C3
        (= :running status)
        {:directives [[:log {:level :warn :event :cannot-cancel-running :data task-id}]] :state state}
        :else
        {:directives [[:log {:level :warn :event :invalid-cancel :data task-id}]] :state state})

      ;; --- X: REPL Clear Throttle ---
      ;; Lift the window and mark throttle-tagged waiters due, so the
      ;; scan-deadlines post-step drains both the queued tasks and the 429'd task
      ;; in this same pass.
      :repl/clear-throttle
      {:directives []
       :state (-> state
                  (assoc :throttle-expires-at nil)
                  (update :tasks
                          (fn [ts]
                            (into {}
                                  (map (fn [[id t]]
                                         [id (if (and (= :waiting-retry (:status t))
                                                      (:throttle? t))
                                               (assoc t :wake-at now)
                                               t)]))
                                  ts))))}

      ;; --- P: REPL Prune ---
      ;; The payload has no :task-id, so task-id/t/status above are nil here —
      ;; fine, this branch doesn't use them.
      :repl/prune
      (let [statuses (into #{} (filter terminal-statuses) (:statuses payload))
            doomed   (->> (:tasks state) vals
                          (filter #(statuses (:status %)))
                          (map :task-id))]
        {:directives (into [[:log {:level :info :event :pruned
                                   :data {:count (count doomed)}}]]
                           (map (fn [id] [:drop-cf {:task-id id}]))
                           doomed)
         :state (update state :tasks #(apply dissoc % doomed))})

      ;; --- Z: Shutdown ---
      :shutdown
      (case (:mode payload)
        :abort-pending
        (let [non-terminals (->> (:tasks state) vals (remove #(terminal-statuses (:status %))))
              dirs          (mapv (fn [task]
                                    [:abort {:task-id (:task-id task)
                                             :error   (ex-info "supervisor stopped" {:type ::shutdown})}])
                                  non-terminals)
              state'        (reduce (fn [s task] (assoc-in s [:tasks (:task-id task) :status] :aborted))
                                    state non-terminals)]
          {:directives dirs :state (assoc state' :shutdown? true)})

        :drop
        {:directives [] :state (assoc state :shutdown? true)})

      ;; --- W1: Wildcard ---
      {:directives [[:log {:level :error :event :unknown-event :data event-type}]] :state state})))

(defn- scan-deadlines
  "Runs after every event (including ticks). Checks the clock against state
   deadlines and drains tasks accordingly.
   Corresponds to rows T-a, T-b, and T-c in the spec.
   Takes and returns a {:directives [...] :state {...}} map."
  [{:keys [directives state]} now]
  (let [;; 1. Remove throttle if naturally expired
        throttle-at (:throttle-expires-at state)
        state-1     (if (and throttle-at (<= throttle-at now))
                      (assoc state :throttle-expires-at nil)
                      state)

        ;; 2. Determine if we are currently throttled
        throttled?  (boolean (:throttle-expires-at state-1))

        ;; 3. Find due tasks (T-a & T-b)
        ;; If we are unthrottled, we drain BOTH queued tasks (T-a / REPL clears)
        ;; AND any retries whose wake-at has arrived (T-b).
        tasks-to-run (if throttled?
                       []
                       (->> (:tasks state-1)
                            vals
                            (filter (fn [t]
                                      (or (= :queued (:status t))
                                          (and (= :waiting-retry (:status t))
                                               (:wake-at t)
                                               (<= (:wake-at t) now)))))))

        ;; 4. Generate execute directives
        new-dirs (map (fn [t] [:execute (select-keys t [:task-id :context :closure])])
                      tasks-to-run)

        ;; 5. Advance states to :running (clear :throttle?)
        state-2  (reduce (fn [s t]
                           (update-in s [:tasks (:task-id t)]
                                      assoc :status :running :wake-at nil :throttle? nil))
                         state-1
                         tasks-to-run)]

    ;; T-c (expiry/TTL logic) is reserved for v2.
    {:directives (into [] (concat directives new-dirs))
     :state state-2}))

;; Top-level
(defn make-reference-policy
  "Builds a pure reference policy `(fn [event state] -> {:directives :state})`,
   closing over `opts`. Recognized opts (others ignored):
     :classify-error      (fn [error] -> :fatal | :rate-limited | :transient)
     :backoff-fn          (fn [attempts] -> backoff-ms) for transient retries
     :max-attempts        default max attempts when a task's context omits
                          :dj.concurrency/max-attempts (default 3)
     :default-throttle-ms throttle window for a 429 with no :retry-after (ms)
   See `default-reference-opts`.

   This is how supervisor-level configuration threads into the (otherwise pure,
   2-arg) policy: config is baked in at construction time rather than passed on
   every event. `create-supervisor` calls this with its own opts when you don't
   supply an explicit :policy."
  [opts]
  (let [config (merge default-reference-opts
                      (select-keys opts [:classify-error :backoff-fn
                                         :max-attempts :default-throttle-ms]))]
    (fn reference-policy [event state]
      (let [;; Fallback to current time only for test/REPL ergonomics. During
            ;; real usage the impure shell stamps :now onto every event.
            now (:now (second event) (System/currentTimeMillis))]
        (-> (apply-event config event state now)
            (scan-deadlines now))))))

(def default-policy
  "The reference policy built with all defaults; equals `(make-reference-policy {})`."
  (make-reference-policy {}))
