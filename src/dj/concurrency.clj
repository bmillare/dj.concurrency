(ns dj.concurrency
  (:import [java.util.concurrent BlockingQueue LinkedBlockingQueue CompletableFuture TimeUnit TimeoutException ExecutionException]
           [clojure.lang IDeref IBlockingDeref IPending]))

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
  (let [wake-ats    (keep :wake-at (vals (:tasks state))) ;; seq of ts
        throttle-at (:throttle-expires-at state) ;; ts or nil
        deadlines   (if throttle-at
                      (conj wake-ats throttle-at)
                      wake-ats)]
    (if (seq deadlines)
      (max 0 (- (apply min deadlines) now))
      Long/MAX_VALUE)))

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

    :log
    (do
      (println (:level payload :info) 
               (str "[MF] " (name (:event payload)) " - " (:data payload)))
      cf-registry)))

;; Supervisor main loop
(defn run-shell!
  "Starts the supervisor's single-threaded event loop on a Virtual Thread.
   `sup` must contain: {:queue BlockingQueue, :state Atom, :policy IFn}"
  [{:keys [^BlockingQueue queue state policy] :as sup}]
  (Thread/startVirtualThread
   (fn []
     (loop [cf-registry {}] ;; task-id -> CF: Track CFs out of state to keep state pure
       (let [;; 1. Get event
             now     (System/currentTimeMillis)
             timeout (ms-until-next-deadline @state now)
             raw-event (.poll queue timeout TimeUnit/MILLISECONDS) ;; wait for event or timeout
             event     (if raw-event
                         [(first raw-event) (assoc (second raw-event) :now now)] ;; Time is plumbed in
                         [:tick {:now now}])
             
             ;; 2. Handle/Extract CF
             [clean-event next-cf-registry]
             (if (= :submit (first event))
               ;; Pluck live CF out of payload to keep policy pure
               (let [payload       (second event)
                     cf            (:cf payload)
                     clean-payload (dissoc payload :cf)]
                 [[:submit clean-payload] (if cf 
                                            (assoc cf-registry (:task-id payload) cf) 
                                            cf-registry)])
               [event cf-registry])

             ;; 3. Get directives + state transition from policy
             [directives state']
             (try
               (policy clean-event @state)
               (catch Throwable t ;; Fallback: empty directives and log on policy throw
                 (println t "[MF-FATAL] Policy threw an exception on event:" clean-event)
                 [[] @state]))]

         ;; 4. Commit state
         (reset! state state')

         ;; 5. Execute directives sequentially
         (let [final-registry (reduce (fn [reg dir]
                                        (execute-directive! sup reg dir))
                                      next-cf-registry
                                      directives)]
           
           ;; 6. Recur or terminate
           ;; - Do we want to have a shutdown option before we execute directives?
           (when-not (:shutdown? state')
             (recur final-registry))))))))


;; =============================================================================
;; User API
;; =============================================================================

(declare default-policy)

(defn create-supervisor
  "Creates and starts a new Manageable Futures supervisor.
   
   Options:
     :policy - A pure policy function `(event, state) -> [directives state']`.
     :name   - Optional name for logging/identification.
     
   Returns a map representing the supervisor, containing its queue, state atom,
   and the running shell thread. This map can easily be integrated into 
   Component/Integrant lifecycles."
  [opts]
  (let [q      (LinkedBlockingQueue.)
        state  (atom {:tasks               {}
                      :throttle-expires-at nil
                      :shutdown?           false})
        policy (:policy opts default-policy) 
        sup    {:queue  q
                :state  state
                :policy policy
                :name   (:name opts)}]
    (assoc sup :shell-thread (run-shell! sup))))

(defn submit
  "Submits a closure to the supervisor for execution.
   Returns a ManageableFuture immediately.
   
   Throws if the supervisor is shutting down or already shut down."
  [supervisor context closure]
  ;; Design decision RI-12 §5: Stop-then-submit is a programming error.
  ;; Reject immediately at the call site rather than enqueueing.
  (if (:shutdown? @(:state supervisor))
    (throw (ex-info "Supervisor is stopped or shutting down" {:type :mf/shutdown}))
    
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
                                
   Returns the supervisor."
  ([supervisor]
   (stop! supervisor :abort-pending))
  ([supervisor mode]
   (.put ^BlockingQueue (:queue supervisor)
         [:shutdown {:mode mode}])
   supervisor))

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


;; --- Interventions ---

(defn retry
  "Requests immediate re-execution of a parked, waiting, or queued task."
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
   The consumer thread remains blocked until its own deref timeout occurs."
  ([stub] 
   (cancel (:supervisor stub) (:task-id stub)))
  ([sup task-id]
   (.put ^BlockingQueue (:queue sup) [:repl/cancel {:task-id task-id}])
   :queued))

(defn clear-throttle
  "Manually lifts a supervisor-wide throttle window, draining the deferred queue."
  [sup]
  (.put ^BlockingQueue (:queue sup) [:repl/clear-throttle {}])
  :queued)

;; =============================================================================
;; Reference Policy
;; =============================================================================

(def ^:private terminal-statuses #{:resolved :aborted :cancelled})

(defn- classify-error
  "Reference heuristic for classifying errors based on the dispatch table examples.
   In a production system, this could delegate to a user-provided function."
  [error]
  (let [d (ex-data error)]
    (cond
      (= (:type d) :business-error) :fatal
      (= (:status d) 429)           :rate-limited
      :else                         :transient)))

(defn- calculate-backoff
  "Simple exponential backoff (1s, 2s, 4s...) based on attempts."
  [attempts]
  (long (* 1000 (Math/pow 2 (dec attempts)))))

;; Event transition logic
(defn- apply-event 
  "Evaluates a single event against the current state.
   Returns a tuple of [new-directives new-state]."
  [[event-type payload] state now]
  (let [task-id (:task-id payload)
        t       (get-in state [:tasks task-id])
        status  (:status t)]
    
    (case event-type
      
      ;; --- S: Submit ---
      :submit
      (if (:shutdown? state)
        ;; S1: Supervisor stopped
        [[[:abort {:task-id task-id 
                   :error (ex-info "supervisor stopped" {:type :mf/shutdown})}]]
         state]
        
        (let [throttled? (and (:throttle-expires-at state) 
                              (< now (:throttle-expires-at state)))]
          (if throttled?
            ;; S2: Throttled
            [[[:log {:level :info :event :submit-throttled :data task-id}]]
             (assoc-in state [:tasks task-id] 
                       {:task-id task-id, :status :queued
                        :context (:context payload), :closure (:closure payload)
                        :submitted-at (:submitted-at payload)})]
            ;; S3: Normal Submit
            (let [ctx' (assoc (:context payload) :mf/attempts 1)]
              [[[:execute {:task-id task-id :context ctx' :closure (:closure payload)}]
                [:log {:level :debug :event :submit-executed :data task-id}]]
               (assoc-in state [:tasks task-id]
                         {:task-id task-id, :status :running
                          :context ctx', :closure (:closure payload)
                          :submitted-at (:submitted-at payload)})]))))

      ;; --- K: Success ---
      :success
      (cond
        ;; K1: Late/Ignored
        (or (nil? t) (terminal-statuses status))
        [[[:log {:level :warn :event :late-success :data task-id}]] state]
        
        ;; K2: Expected Success
        (= :running status)
        [[[:resolve {:task-id task-id :result (:result payload)}]]
         (assoc-in state [:tasks task-id :status] :resolved)]
        
        ;; K3: Illegal
        :else
        [[[:log {:level :warn :event :illegal-transition :data task-id}]] state])

      ;; --- F: Failed ---
      :failed
      (cond
        ;; F1: Late/Ignored
        (or (nil? t) (terminal-statuses status))
        [[[:log {:level :warn :event :late-failure :data task-id}]] state]
        
        (= :running status)
        (let [err-type     (classify-error (:error payload))
              attempts     (get-in t [:context :mf/attempts] 1)
              max-attempts (get-in t [:context :mf/max-attempts] 3)]
          (case err-type
            ;; F2: Fatal
            :fatal
            [[[:abort {:task-id task-id :error (:error payload)}]]
             (update-in state [:tasks task-id] assoc :status :aborted :error (:error payload))]
            
            ;; F3: Rate Limited (429)
            :rate-limited
            (let [window  (or (:retry-after (ex-data (:error payload))) 5000)
                  wake-at (+ now window)]
              [[]
               (-> state
                   (assoc :throttle-expires-at wake-at)
                   (update-in [:tasks task-id] assoc 
                              :status :waiting-retry :wake-at wake-at :error (:error payload))
                   (update-in [:tasks task-id :context :mf/attempts] inc))])
            
            ;; F4 & F5: Transient
            :transient
            (if (< attempts max-attempts)
              ;; F4: Retryable
              (let [backoff (calculate-backoff attempts)]
                [[]
                 (-> state
                     (update-in [:tasks task-id] assoc 
                                :status :waiting-retry :wake-at (+ now backoff) :error (:error payload))
                     (update-in [:tasks task-id :context :mf/attempts] inc))])
              
              ;; F5: Exhausted / Parked
              [[[:log {:level :info :event :parked :data task-id}]]
               (update-in state [:tasks task-id] assoc :status :parked :error (:error payload))])))
        
        ;; F6: Illegal
        :else
        [[[:log {:level :warn :event :illegal-transition :data task-id}]] state])

      ;; --- T: Tick ---
      :tick
      [[] state] ;; Handled purely by the deadline-scan post-step

      ;; --- R: REPL Retry ---
      :repl/retry
      (cond
        ;; R1
        (#{:parked :waiting-retry :queued} status)
        [[[:execute (select-keys t [:task-id :context :closure])]] 
         (update-in state [:tasks task-id] assoc :status :running :wake-at nil)]
        ;; R2 & R3
        (= :running status)
        [[[:log {:level :warn :event :already-running :data task-id}]] state]
        :else
        [[[:log {:level :warn :event :no-such-task :data task-id}]] state])

      ;; --- D: REPL Deliver ---
      :repl/deliver
      (if (#{:parked :waiting-retry :queued :running} status)
        [[[:resolve {:task-id task-id :result (:result payload)}]]
         (assoc-in state [:tasks task-id :status] :resolved)]
        [[[:log {:level :warn :event :invalid-deliver :data task-id}]] state])

      ;; --- A: REPL Abort ---
      :repl/abort
      (if (#{:parked :waiting-retry :queued :running} status)
        [[[:abort {:task-id task-id :error (:error payload)}]]
         (assoc-in state [:tasks task-id :status] :aborted)]
        [[[:log {:level :warn :event :invalid-abort :data task-id}]] state])

      ;; --- C: REPL Cancel ---
      :repl/cancel
      (cond
        ;; C1
        (#{:parked :waiting-retry :queued} status)
        [[[:log {:level :info :event :cancelled :data task-id}]]
         (assoc-in state [:tasks task-id :status] :cancelled)]
        ;; C2 & C3
        (= :running status)
        [[[:log {:level :warn :event :cannot-cancel-running :data task-id}]] state]
        :else
        [[[:log {:level :warn :event :invalid-cancel :data task-id}]] state])

      ;; --- X: REPL Clear Throttle ---
      :repl/clear-throttle
      [[] (assoc state :throttle-expires-at nil)] ;; Draining happens in post-step

      ;; --- Z: Shutdown ---
      :shutdown
      (case (:mode payload)
        :abort-pending
        (let [non-terminals (->> (:tasks state) vals (remove #(terminal-statuses (:status %))))
              dirs          (map (fn [task] 
                                   [:abort {:task-id (:task-id task)
                                            :error   (ex-info "supervisor stopped" {:type :mf/shutdown})}])
                                 non-terminals)
              state'        (reduce (fn [s task] (assoc-in s [:tasks (:task-id task) :status] :aborted))
                                    state non-terminals)]
          [dirs (assoc state' :shutdown? true)])
        
        :drop
        [[] (assoc state :shutdown? true)])

      ;; --- W1: Wildcard ---
      [[[:log {:level :error :event :unknown-event :data event-type}]] state])))

(defn- scan-deadlines 
  "Runs after every event (including ticks). Checks the clock against state 
   deadlines and drains tasks accordingly. 
   Corresponds to rows T-a, T-b, and T-c in the spec."
  [directives state now]
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
        
        ;; 5. Advance states to :running
        state-2  (reduce (fn [s t]
                           (update-in s [:tasks (:task-id t)] 
                                      assoc :status :running :wake-at nil))
                         state-1
                         tasks-to-run)]
    
    ;; T-c (expiry/TTL logic) is reserved for v2.
    [(into [] (concat directives new-dirs)) state-2]))

;; Top-level
(defn default-policy
  "The pure, referentially-transparent state machine for the supervisor.
   Given an event `[type payload]` and a state map, returns `[directives state']`."
  [event state]
  (let [;; Fallback to current time only for test REPL ergonomics.
        ;; During real usage, the impure shell stamps :now onto every event.
        now (:now (second event) (System/currentTimeMillis))
        
        ;; 1. Process the specific event logic
        [dirs state'] (apply-event event state now)]
    
    ;; 2. Process deadline/time triggers 
    (scan-deadlines dirs state' now)))
