(ns dj.concurrency-test
  "Basic tests for dj.concurrency.

   Two layers:
   - Pure policy tests drive `default-policy` directly with an explicit `:now`,
     so retry/throttle/park scheduling is deterministic and instant.
   - Integration tests exercise a live supervisor (real virtual threads) for the
     happy path and the REPL-driven recovery workflow."
  (:require [clojure.test :refer [deftest testing is]]
            [dj.concurrency :as c]))

;; =============================================================================
;; Helpers
;; =============================================================================

(def ^:private base-state
  {:tasks {} :throttle-expires-at nil :shutdown? false})

(defn- dir-types
  "Set of directive types in a directive vector."
  [directives]
  (set (map first directives)))

(defn- find-dir
  "First directive of the given type, or nil."
  [directives dir-type]
  (first (filter #(= dir-type (first %)) directives)))

(defn- running-task
  "A task map already in :running state with the given context, for feeding
   :success / :failed events in pure tests."
  [task-id context]
  {:task-id task-id :status :running
   :context context :closure (fn [] :noop) :submitted-at 0})

(defn- wait-for
  "Poll `pred` until truthy or timeout. Returns true/false."
  ([pred] (wait-for pred 2000))
  ([pred timeout-ms]
   (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
     (loop []
       (cond
         (pred) true
         (> (System/currentTimeMillis) deadline) false
         :else (do (Thread/sleep 10) (recur)))))))

(defn- status-of [stub]
  (:status (c/task stub)))

;; =============================================================================
;; Pure policy tests (deterministic, no threads)
;; =============================================================================

(deftest submit-normal
  (testing "a fresh submit executes immediately and the task is running"
    (let [{dirs :directives state' :state}
          (c/default-policy
           [:submit {:task-id "t1" :context {:foo 1}
                     :closure (fn [] :ok) :submitted-at 0 :now 1000}]
           base-state)]
      (is (contains? (dir-types dirs) :execute))
      (is (= :running (get-in state' [:tasks "t1" :status])))
      (is (= 1 (get-in state' [:tasks "t1" :context ::c/attempts]))
          "attempts is initialized to 1"))))

(deftest success-resolves
  (testing "success on a running task resolves the future"
    (let [state (assoc-in base-state [:tasks "t1"] (running-task "t1" {::c/attempts 1}))
          {dirs :directives state' :state}
          (c/default-policy [:success {:task-id "t1" :result 42 :now 2000}] state)]
      (is (= [:resolve {:task-id "t1" :result 42}] (find-dir dirs :resolve)))
      (is (= :resolved (get-in state' [:tasks "t1" :status]))))))

(deftest transient-failure-schedules-retry
  (testing "a transient failure below max-attempts schedules a backed-off retry"
    (let [state (assoc-in base-state [:tasks "t1"] (running-task "t1" {::c/attempts 1}))
          {dirs :directives state' :state}
          (c/default-policy [:failed {:task-id "t1" :error (ex-info "boom" {})
                                      :now 5000}] state)]
      (is (not (contains? (dir-types dirs) :execute))
          "no immediate re-execute; it waits for the backoff window")
      (is (= :waiting-retry (get-in state' [:tasks "t1" :status])))
      (is (= 6000 (get-in state' [:tasks "t1" :wake-at]))
          "wake-at = now + 1s backoff for attempt 1")
      (is (= 2 (get-in state' [:tasks "t1" :context ::c/attempts]))))))

(deftest transient-failure-exhausted-parks
  (testing "a transient failure at max-attempts parks the task for REPL recovery"
    (let [state (assoc-in base-state [:tasks "t1"]
                          (running-task "t1" {::c/attempts 3 ::c/max-attempts 3}))
          {state' :state}
          (c/default-policy [:failed {:task-id "t1" :error (ex-info "boom" {})
                                      :now 5000}] state)]
      (is (= :parked (get-in state' [:tasks "t1" :status]))))))

(deftest rate-limited-throttles-supervisor
  (testing "a 429 sets a supervisor-wide throttle window"
    (let [state (assoc-in base-state [:tasks "t1"] (running-task "t1" {::c/attempts 1}))
          {state' :state}
          (c/default-policy [:failed {:task-id "t1"
                                      :error (ex-info "rl" {:status 429 :retry-after 3000})
                                      :now 1000}] state)]
      (is (= 4000 (:throttle-expires-at state')) "throttle = now + retry-after")
      (is (= :waiting-retry (get-in state' [:tasks "t1" :status]))))))

(deftest fatal-failure-aborts
  (testing "a fatal (business) error aborts immediately, no retry"
    (let [state (assoc-in base-state [:tasks "t1"] (running-task "t1" {::c/attempts 1}))
          {dirs :directives state' :state}
          (c/default-policy [:failed {:task-id "t1"
                                      :error (ex-info "bad input" {:type :business-error})
                                      :now 1000}] state)]
      (is (contains? (dir-types dirs) :abort))
      (is (= :aborted (get-in state' [:tasks "t1" :status]))))))

(deftest submit-while-throttled-queues
  (testing "submitting during a throttle window queues instead of executing"
    (let [state (assoc base-state :throttle-expires-at 10000)
          {dirs :directives state' :state}
          (c/default-policy [:submit {:task-id "t2" :context {} :closure (fn [] :ok)
                                      :submitted-at 0 :now 5000}] state)]
      (is (not (contains? (dir-types dirs) :execute)))
      (is (= :queued (get-in state' [:tasks "t2" :status]))))))

(deftest deadline-scan-drains-due-retry
  (testing "a tick at/after wake-at re-executes a waiting-retry task"
    (let [state (assoc-in base-state [:tasks "t1"]
                          (assoc (running-task "t1" {::c/attempts 2})
                                 :status :waiting-retry :wake-at 6000))
          {dirs :directives state' :state}
          (c/default-policy [:tick {:now 6000}] state)]
      (is (contains? (dir-types dirs) :execute))
      (is (= :running (get-in state' [:tasks "t1" :status])))
      (is (nil? (get-in state' [:tasks "t1" :wake-at]))))))

(deftest clear-throttle-drains-queued
  (testing "clearing the throttle immediately drains queued tasks"
    (let [state (-> base-state
                    (assoc :throttle-expires-at 10000)
                    (assoc-in [:tasks "t2"]
                              (assoc (running-task "t2" {}) :status :queued)))
          {dirs :directives state' :state}
          (c/default-policy [:repl/clear-throttle {:now 5000}] state)]
      (is (nil? (:throttle-expires-at state')))
      (is (contains? (dir-types dirs) :execute))
      (is (= :running (get-in state' [:tasks "t2" :status]))))))

(deftest repl-interventions-on-parked
  (let [parked (fn [] (assoc-in base-state [:tasks "t1"]
                                (assoc (running-task "t1" {}) :status :parked)))]
    (testing "deliver resolves a parked task"
      (let [{dirs :directives state' :state}
            (c/default-policy [:repl/deliver {:task-id "t1" :result :mock :now 1}] (parked))]
        (is (= [:resolve {:task-id "t1" :result :mock}] (find-dir dirs :resolve)))
        (is (= :resolved (get-in state' [:tasks "t1" :status])))))
    (testing "abort fails a parked task"
      (let [{dirs :directives state' :state}
            (c/default-policy [:repl/abort {:task-id "t1" :error (ex-info "no" {}) :now 1}]
                              (parked))]
        (is (contains? (dir-types dirs) :abort))
        (is (= :aborted (get-in state' [:tasks "t1" :status])))))
    (testing "retry re-executes a parked task"
      (let [{dirs :directives state' :state}
            (c/default-policy [:repl/retry {:task-id "t1" :now 1}] (parked))]
        (is (contains? (dir-types dirs) :execute))
        (is (= :running (get-in state' [:tasks "t1" :status])))))
    (testing "cancel discards a parked task"
      (let [{state' :state}
            (c/default-policy [:repl/cancel {:task-id "t1" :now 1}] (parked))]
        (is (= :cancelled (get-in state' [:tasks "t1" :status])))))))

(deftest configurable-backoff
  (testing "make-reference-policy threads a custom :backoff-fn into retries"
    (let [policy (c/make-reference-policy {:backoff-fn (fn [_] 50)})
          state  (assoc-in base-state [:tasks "t1"] (running-task "t1" {::c/attempts 1}))
          {state' :state}
          (policy [:failed {:task-id "t1" :error (ex-info "boom" {}) :now 1000}] state)]
      (is (= 1050 (get-in state' [:tasks "t1" :wake-at])) "now + custom 50ms backoff"))))

(deftest configurable-max-attempts
  (testing "make-reference-policy threads a custom :max-attempts default"
    (let [policy (c/make-reference-policy {:max-attempts 1})
          state  (assoc-in base-state [:tasks "t1"] (running-task "t1" {::c/attempts 1}))
          {state' :state}
          (policy [:failed {:task-id "t1" :error (ex-info "boom" {}) :now 1000}] state)]
      (is (= :parked (get-in state' [:tasks "t1" :status]))
          "attempts 1 >= max 1 -> parks immediately"))))

;; =============================================================================
;; Integration tests (live supervisor, real virtual threads)
;; =============================================================================

(deftest happy-path-deref
  (testing "submit/deref returns the closure result"
    (let [sup (c/create-supervisor {})]
      (try
        (is (= 3 @(c/submit sup {} (fn [] (+ 1 2)))))
        (finally (c/stop! sup))))))

(deftest fatal-error-propagates-on-deref
  (testing "a business error surfaces to the blocked consumer on deref"
    (let [sup (c/create-supervisor {})]
      (try
        (let [f (c/submit sup {} (fn [] (throw (ex-info "bad input"
                                                        {:type :business-error}))))]
          (is (thrown-with-msg? clojure.lang.ExceptionInfo #"bad input" @f)))
        (finally (c/stop! sup))))))

(deftest auto-retry-eventually-succeeds
  (testing "a transient failure is retried automatically and then resolves"
    (let [sup     (c/create-supervisor {})
          attempts (atom 0)]
      (try
        (let [f (c/submit sup {}
                          (fn []
                            (if (= 1 (swap! attempts inc))
                              (throw (ex-info "flaky" {})) ;; transient -> retry
                              :recovered)))]
          (is (= :recovered (deref f 5000 :timed-out)))
          (is (= 2 @attempts) "ran twice: one failure + one success"))
        (finally (c/stop! sup))))))

(deftest park-then-repl-deliver
  (testing "an exhausted task parks; deliver-result unblocks the consumer with a mock"
    (let [sup (c/create-supervisor {})]
      (try
        (let [f (c/submit sup {:endpoint :flaky ::c/max-attempts 1}
                          (fn [] (throw (ex-info "still down" {}))))]
          (is (wait-for #(= :parked (status-of f))) "task parks after exhausting retries")
          (is (seq (c/parked-tasks sup)) "shows up in parked-tasks")
          (c/deliver-result f :mocked-result)
          (is (= :mocked-result (deref f 2000 :timed-out))))
        (finally (c/stop! sup))))))

(deftest park-then-repl-retry
  (testing "a parked task can be re-run from the REPL after the world is fixed"
    (let [sup    (c/create-supervisor {})
          fixed? (atom false)]
      (try
        (let [f (c/submit sup {::c/max-attempts 1}
                          (fn [] (if @fixed? :ok (throw (ex-info "down" {})))))]
          (is (wait-for #(= :parked (status-of f))))
          (reset! fixed? true)        ;; "hot-reload" the world
          (c/retry f)
          (is (= :ok (deref f 2000 :timed-out))))
        (finally (c/stop! sup))))))

(deftest stop-aborts-pending
  (testing "stopping the supervisor aborts parked tasks"
    (let [sup (c/create-supervisor {})]
      (try
        (let [f (c/submit sup {::c/max-attempts 1}
                          (fn [] (throw (ex-info "down" {}))))]
          (is (wait-for #(= :parked (status-of f))))
          (c/stop! sup :abort-pending)
          (is (thrown-with-msg? clojure.lang.ExceptionInfo #"supervisor stopped"
                                (deref f 2000 :timed-out))))
        (finally (c/stop! sup))))))

(deftest pluggable-log-fn
  (testing "a custom :log-fn receives log entries (no logging dependency)"
    (let [entries (atom [])
          sup     (c/create-supervisor {:log-fn (fn [e] (swap! entries conj e))})]
      (try
        @(c/submit sup {} (fn [] :ok))
        (is (wait-for #(some (fn [e] (= :submit-executed (:event e))) @entries))
            "the :submit-executed entry was delivered to our log-fn")
        (finally (c/stop! sup))))))

(deftest wait-for-shutdown-blocks-until-stopped
  (testing "wait-for-shutdown returns a promise realized once the shell stops"
    (let [sup (c/create-supervisor {})]
      (is (not (realized? (c/wait-for-shutdown sup))) "not realized while running")
      (c/stop! sup)
      (is (= true (deref (c/wait-for-shutdown sup) 2000 :timed-out))
          "realized true after shutdown completes"))))
