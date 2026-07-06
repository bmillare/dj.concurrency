(ns dj.concurrency.shell
  "Imperative shell for dj.concurrency: the impure runtime that owns the event
   queue, spawns virtual threads, and interprets directives. Everything with a
   side effect lives here; the pure decision-making lives in
   `dj.concurrency.policy`.

   The shell communicates with the policy through two positional pairs that
   travel across the queue verbatim:
     - events     [event-type     payload-map]
     - directives [directive-type payload-map]
   Every other value here is a MAP (policy results, internal shell bindings) so
   the code reads by key rather than by position.

   NOTE: the qualified keyword `:dj.concurrency/shutdown` is part of the public
   contract (consumers match on `(:type (ex-data e))`). It is keyed to the
   `dj.concurrency` namespace, so it is written out in full rather than with
   `::` auto-resolution (which would key it to THIS namespace)."
  (:require [dj.concurrency.store :as store])
  (:import [java.util.concurrent BlockingQueue CompletableFuture
            TimeUnit TimeoutException ExecutionException]
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
  "Invokes the supervisor's :event-tap, swallowing anything it throws.
   A broken tap must never take down the shell loop."
  [sup entry]
  (try ((:event-tap sup) entry)
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
           cf (ex-info "supervisor stopped" {:type :dj.concurrency/shutdown}))))
      (recur))))

(defn- safe-lookup
  "Store lookup that degrades to a miss on any throw."
  [sup task-id s k]
  (try (store/lookup s k)
       (catch Throwable t
         (safe-log! sup {:level :warn :event :store-lookup-failed :task-id task-id
                         :data {:key k :error t}})
         nil)))

(defn- safe-record!
  "Store persist that degrades to 'not memoized' on any throw.
   Blocks until durable on success (persist-then-publish)."
  [sup task-id s k entry]
  (try (store/record! s k entry)
       (catch Throwable t
         (safe-log! sup {:level :warn :event :store-record-failed :task-id task-id
                         :data {:key k :error t}}))))

(defn- execute-worker-wrapper
  "Code that runs on a VirtualThread (in execute-directive!). Consults the
   supervisor's optional ResultStore for tasks carrying a durable key: a hit
   short-circuits the closure; a miss runs it and durably records the result
   BEFORE publishing :success (persist-then-publish). Store failures degrade
   to no-cache and never fail the task. Routes results back to the
   supervisor's single event queue."
  [{:keys [^BlockingQueue queue store] :as sup} {:keys [task-id context closure]}]
  (let [start (System/currentTimeMillis)
        k     (:dj.concurrency/durable-key context)]
    (try
      (if-let [hit (when (and store k) (safe-lookup sup task-id store k))]
        (.put queue [:success {:task-id  task-id
                               :result   (:result hit)
                               :cached?  true
                               :duration (- (System/currentTimeMillis) start)}])
        (let [result (closure)]
          (when (and store k)
            (safe-record! sup task-id store k {:result result}))
          (.put queue [:success {:task-id  task-id
                                 :result   result
                                 :duration (- (System/currentTimeMillis) start)}])))
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
      ;; supervisor's :event-tap. Default is `default-event-tap` (a loud dev
      ;; breadcrumb to *err*), so we never depend on or fight with a logging
      ;; system. Override via {:event-tap ...} on create-supervisor (pass `tap>`
      ;; for the old silent default).
      (safe-log! sup payload)
      cf-registry)))

;; Supervisor main loop
(defn run-shell!
  "Starts the supervisor's single-threaded event loop on a Virtual Thread.
   `sup` must contain: {:queue BlockingQueue, :state Atom, :policy IFn,
   :event-tap IFn, :shutdown-promise promise}.

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
