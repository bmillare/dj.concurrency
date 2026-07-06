(ns dj.concurrency-test
  "Basic tests for dj.concurrency.

   Two layers:
   - Pure policy tests drive `default-policy` directly with an explicit `:now`,
     so retry/throttle/park scheduling is deterministic and instant.
   - Integration tests exercise a live supervisor (real virtual threads) for the
     happy path and the REPL-driven recovery workflow."
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.datafy :as datafy]
            [clojure.string :as str]
            [dj.concurrency :as c]
            [dj.concurrency.store :as store]
            ;; a couple of private shell internals are poked directly below
            [dj.concurrency.shell :as shell])
  (:import [java.util.concurrent LinkedBlockingQueue CompletableFuture ExecutionException
            CountDownLatch]))

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
      (is (= 2 (get-in state' [:tasks "t1" :context ::c/attempts])))
      ;; F1: the retry is a first-class event so a retry-storm is visible on the tap
      (let [[_ log] (find-dir dirs :log)]
        (is (= :retry-scheduled (:event log)) "emits :retry-scheduled")
        (is (= 2 (get-in log [:data :attempt])) ":attempt is the NEXT attempt number")
        (is (= 1000 (get-in log [:data :wake-in-ms])) ":wake-in-ms is the backoff")))))

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
          {dirs :directives state' :state}
          (c/default-policy [:failed {:task-id "t1"
                                      :error (ex-info "rl" {:status 429 :retry-after 3000})
                                      :now 1000}] state)]
      (is (= 4000 (:throttle-expires-at state')) "throttle = now + retry-after")
      (is (= :waiting-retry (get-in state' [:tasks "t1" :status])))
      ;; F1: the throttle is a first-class, supervisor-level event on the tap
      (let [[_ log] (find-dir dirs :log)]
        (is (= :throttle-wait (:event log)) "emits :throttle-wait")
        (is (= :info (:level log)) "supervisor-wide pause is :info, not :debug")
        (is (= 3000 (get-in log [:data :wake-in-ms])) ":wake-in-ms is the throttle window")))))

(deftest entries-carry-top-level-task-id
  (testing "task-scoped events expose (:task-id entry) as the reliable reactor handle,
            regardless of whether :data is a bare id or a map"
    ;; bare-:data event (:parked) — data IS the id, top-level mirrors it
    (let [state (assoc-in base-state [:tasks "t1"]
                          (running-task "t1" {::c/attempts 3 ::c/max-attempts 3}))
          {dirs :directives} (c/default-policy [:failed {:task-id "t1"
                                                         :error (ex-info "boom" {})
                                                         :now 1}] state)
          [_ log] (find-dir dirs :log)]
      (is (= :parked (:event log)))
      (is (= "t1" (:task-id log)) "top-level :task-id present on a bare-:data event"))
    ;; map-:data event (:retry-scheduled) — top-level :task-id alongside the map
    (let [state (assoc-in base-state [:tasks "t2"] (running-task "t2" {::c/attempts 1}))
          {dirs :directives} (c/default-policy [:failed {:task-id "t2"
                                                         :error (ex-info "boom" {})
                                                         :now 1}] state)
          [_ log] (find-dir dirs :log)]
      (is (= :retry-scheduled (:event log)))
      (is (= "t2" (:task-id log)) "top-level :task-id present on a map-:data event"))
    ;; misuse event (:no-such-task) is still task-scoped — the id the caller asked for
    (let [{dirs :directives} (c/default-policy [:repl/retry {:task-id "ghost" :now 1}]
                                               base-state)
          [_ log] (find-dir dirs :log)]
      (is (= :no-such-task (:event log)))
      (is (= "ghost" (:task-id log))))))

(deftest tasks-by-status-groups-the-poll-surface
  (testing "tasks-by-status groups tasks by :status for a reconcile loop"
    (let [sup {:state (atom {:tasks {"a" {:task-id "a" :status :parked}
                                     "b" {:task-id "b" :status :parked}
                                     "c" {:task-id "c" :status :running}
                                     "d" {:task-id "d" :status :queued}}})}
          by  (c/tasks-by-status sup)]
      (is (= #{:parked :running :queued} (set (keys by))))
      (is (= 2 (count (:parked by))))
      (is (= #{"a" "b"} (set (map :task-id (:parked by)))))
      ;; agrees with the park-only shortcut
      (is (= (set (keys (c/parked-tasks sup)))
             (set (map :task-id (:parked by))))))))

(deftest explain-stuck-summarizes-parked-tasks
  (testing "explain-stuck condenses the parked set into a flat, pprint-friendly summary"
    (let [sup {:state (atom {:tasks
                             {"old" {:task-id "old" :status :parked :submitted-at 0
                                     :error (ex-info "upstream 503" {:type :transient})
                                     :context {:dj.concurrency/attempts 5
                                               :dj.concurrency/durable-key "summarize:doc-42"}}
                              "new" {:task-id "new" :status :parked :submitted-at 1000
                                     :error (ex-info "connection reset" {:type :transient})
                                     :context {:dj.concurrency/attempts 5}}
                              ;; non-parked tasks are excluded from the summary
                              "run" {:task-id "run" :status :running :submitted-at 0}}})}
          {:keys [parked-count tasks]} (c/explain-stuck sup)]
      (is (= 2 parked-count) "counts only parked tasks")
      (is (= 2 (count tasks)))
      (is (= ["old" "new"] (mapv :task-id tasks)) "sorted oldest-first (largest :age-ms)")
      (let [t (first tasks)]
        (is (= 5 (:attempts t)) "attempts pulled from internal :context key path")
        (is (= "upstream 503" (:error t)) "error is the message string, not the Throwable")
        (is (= :transient (:error-type t)) "classifier tag pulled from ex-data")
        (is (= "summarize:doc-42" (:durable-key t)))
        (is (pos? (:age-ms t)) "age derived from :submitted-at"))
      (is (nil? (:durable-key (second tasks))) ":durable-key is nil when the task isn't memoized")
      ;; the whole point: no Throwable / closure leaks — every value is pprint-safe
      (is (every? (fn [t] (every? #(or (nil? %) (string? %) (number? %) (keyword? %))
                                  (vals t)))
                  tasks)
          "every field value is a plain string/number/keyword/nil"))))

(deftest explain-stuck-empty
  (testing "no parked tasks -> {:parked-count 0 :tasks []}"
    (let [sup {:state (atom {:tasks {"r" {:task-id "r" :status :running}}})}]
      (is (= {:parked-count 0 :tasks []} (c/explain-stuck sup))))))

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

;; --- alpha2: correctness fixes + decisions (pure) ---

(deftest interventions-clear-wake-at
  (testing "deliver/abort/cancel out of :waiting-retry clear :wake-at (1.1b, no busy-loop)"
    (let [waiting (assoc-in base-state [:tasks "t1"]
                            (assoc (running-task "t1" {::c/attempts 2})
                                   :status :waiting-retry :wake-at 6000))]
      (doseq [[etype payload] [[:repl/deliver {:task-id "t1" :result :x :now 9999}]
                               [:repl/abort {:task-id "t1" :error (ex-info "no" {}) :now 9999}]
                               [:repl/cancel {:task-id "t1" :now 9999}]]]
        (let [{state' :state} (c/default-policy [etype payload] waiting)]
          (is (nil? (get-in state' [:tasks "t1" :wake-at]))
              (str etype " clears wake-at")))))))

(deftest deadline-scan-ignores-terminal
  (testing "a terminal task with a stale :wake-at does not drive the poll timeout (1.1a)"
    (let [state (assoc-in base-state [:tasks "t1"]
                          (assoc (running-task "t1" {}) :status :resolved :wake-at 1))]
      (is (= Long/MAX_VALUE (#'shell/ms-until-next-deadline state 10000))))))

(deftest throttled-submit-seeds-attempts
  (testing "a throttled submit is queued with ::attempts 1; a later transient failure increments cleanly (1.2)"
    (let [throttled (assoc base-state :throttle-expires-at 10000)
          {s1 :state} (c/default-policy
                       [:submit {:task-id "t1" :context {} :closure (fn [] :ok)
                                 :submitted-at 0 :now 5000}] throttled)]
      (is (= 1 (get-in s1 [:tasks "t1" :context ::c/attempts]))
          "queued task seeded with attempts 1")
      ;; drain: a tick at/after the throttle expiry advances it to :running
      (let [{s2 :state} (c/default-policy [:tick {:now 10000}] s1)]
        (is (= :running (get-in s2 [:tasks "t1" :status])))
        ;; transient failure must not throw and must increment attempts
        (let [{s3 :state} (c/default-policy
                           [:failed {:task-id "t1" :error (ex-info "boom" {}) :now 10001}] s2)]
          (is (= 2 (get-in s3 [:tasks "t1" :context ::c/attempts])))
          (is (= :waiting-retry (get-in s3 [:tasks "t1" :status]))))))))

(deftest prune-removes-only-terminal
  (testing "prune drops terminal tasks, keeps non-terminal, emits one log + N drop-cf"
    (let [mk    (fn [id st] [id (assoc (running-task id {}) :status st)])
          state (assoc base-state :tasks
                       (into {} [(mk "r" :resolved) (mk "a" :aborted) (mk "c" :cancelled)
                                 (mk "p" :parked) (mk "w" :waiting-retry) (mk "run" :running)]))
          {dirs :directives state' :state}
          (c/default-policy [:repl/prune {:statuses #{:resolved :aborted :cancelled} :now 1}] state)]
      (is (= #{"p" "w" "run"} (set (keys (:tasks state')))) "only non-terminal survive")
      (is (= 3 (count (filter #(= :drop-cf (first %)) dirs))) "one drop-cf per pruned")
      (is (= 1 (count (filter #(= :log (first %)) dirs))) "one summary log")))
  (testing "prune with a non-terminal status set prunes nothing"
    (let [state (assoc-in base-state [:tasks "p"] (assoc (running-task "p" {}) :status :parked))
          {state' :state}
          (c/default-policy [:repl/prune {:statuses #{:parked} :now 1}] state)]
      (is (= #{"p"} (set (keys (:tasks state'))))))))

(deftest decision-1-throttle-keeps-longer-window
  (testing "a second 429 with a shorter retry-after does not shorten the throttle window"
    (let [state (assoc-in base-state [:tasks "t1"] (running-task "t1" {::c/attempts 1}))
          {s1 :state} (c/default-policy
                       [:failed {:task-id "t1"
                                 :error (ex-info "rl" {:status 429 :retry-after 10000})
                                 :now 1000}] state)
          s1'  (assoc-in s1 [:tasks "t2"] (running-task "t2" {::c/attempts 1}))
          {s2 :state} (c/default-policy
                       [:failed {:task-id "t2"
                                 :error (ex-info "rl" {:status 429 :retry-after 1000})
                                 :now 2000}] s1')]
      (is (= 11000 (:throttle-expires-at s2))
          "kept the longer expiry (1000+10000), not the shorter (2000+1000)"))))

(deftest decision-2-429-does-not-consume-attempts
  (testing "a 429 does not count against the retry budget"
    (let [state (assoc-in base-state [:tasks "t1"] (running-task "t1" {::c/attempts 1}))
          {state' :state}
          (c/default-policy [:failed {:task-id "t1"
                                      :error (ex-info "rl" {:status 429 :retry-after 1000})
                                      :now 1000}] state)]
      (is (= 1 (get-in state' [:tasks "t1" :context ::c/attempts]))
          "attempts unchanged by a 429"))))

(deftest decision-3-clear-throttle-wakes-429d-task
  (testing "clear-throttle immediately re-runs the task that received the 429"
    (let [state (assoc-in base-state [:tasks "t1"] (running-task "t1" {::c/attempts 1}))
          {s1 :state} (c/default-policy
                       [:failed {:task-id "t1"
                                 :error (ex-info "rl" {:status 429 :retry-after 60000})
                                 :now 1000}] state)]
      (is (true? (get-in s1 [:tasks "t1" :throttle?])) "429'd task tagged :throttle?")
      (let [{dirs :directives s2 :state}
            (c/default-policy [:repl/clear-throttle {:now 2000}] s1)]
        (is (contains? (dir-types dirs) :execute) "waiter re-executed in the same pass")
        (is (= :running (get-in s2 [:tasks "t1" :status])))
        (is (nil? (get-in s2 [:tasks "t1" :wake-at])))
        (is (nil? (get-in s2 [:tasks "t1" :throttle?])) "throttle? cleared on run")))))

(deftest decision-4-retry-resets-attempts
  (testing "REPL retry grants a fresh budget; the next transient failure retries, not re-parks"
    (let [state (assoc-in base-state [:tasks "t1"]
                          (assoc (running-task "t1" {::c/attempts 3 ::c/max-attempts 3})
                                 :status :parked))
          {s1 :state} (c/default-policy [:repl/retry {:task-id "t1" :now 1000}] state)]
      (is (= 1 (get-in s1 [:tasks "t1" :context ::c/attempts])) "attempts reset to 1")
      (is (= :running (get-in s1 [:tasks "t1" :status])))
      (let [{s2 :state} (c/default-policy
                         [:failed {:task-id "t1" :error (ex-info "boom" {}) :now 1001}] s1)]
        (is (= :waiting-retry (get-in s2 [:tasks "t1" :status]))
            "retries instead of re-parking")))))

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

(deftest pluggable-event-tap
  (testing "a custom :event-tap receives log entries (no logging dependency)"
    (let [entries (atom [])
          sup     (c/create-supervisor {:event-tap (fn [e] (swap! entries conj e))})]
      (try
        @(c/submit sup {} (fn [] :ok))
        (is (wait-for #(some (fn [e] (= :submit-executed (:event e))) @entries))
            "the :submit-executed entry was delivered to our event-tap")
        (finally (c/stop! sup))))))

(deftest wait-for-shutdown-blocks-until-stopped
  (testing "wait-for-shutdown returns a promise realized once the shell stops"
    (let [sup (c/create-supervisor {})]
      (is (not (realized? (c/wait-for-shutdown sup))) "not realized while running")
      (c/stop! sup)
      (is (= true (deref (c/wait-for-shutdown sup) 2000 :timed-out))
          "realized true after shutdown completes"))))

;; --- alpha2: shell/integration ---

(deftest no-busy-loop-after-deliver
  (testing "delivering a waiting-retry task clears its deadline; the shell stays responsive (1.1)"
    (let [sup (c/create-supervisor {})]
      (try
        (let [f (c/submit sup {} (fn [] (throw (ex-info "flaky" {}))))]
          (is (wait-for #(= :waiting-retry (status-of f))) "enters waiting-retry after first failure")
          (c/deliver-result f :mocked)
          (is (= :mocked (deref f 2000 :timed-out)))
          ;; past the original backoff window the shell must still process new work promptly
          (Thread/sleep 1200)
          (let [g (c/submit sup {} (fn [] :pong))]
            (is (= :pong (deref g 2000 :timed-out)) "shell still handles new submits")))
        (finally (c/stop! sup))))))

(deftest throwing-event-tap-survives
  (testing "a throwing :event-tap never takes down the shell (1.4)"
    (let [sup (c/create-supervisor {:event-tap (fn [_] (throw (ex-info "boom" {})))})]
      (is (= 3 (deref (c/submit sup {} (fn [] (+ 1 2))) 2000 :timed-out)))
      (c/stop! sup)
      (is (= true (deref (c/wait-for-shutdown sup) 2000 :timed-out))
          "stop!/wait-for-shutdown still complete"))))

(deftest shutdown-drain-completes-stranded-submit
  (testing "the shutdown drain completes a stranded :submit's CF exceptionally (1.5)"
    (let [q  (LinkedBlockingQueue.)
          cf (CompletableFuture.)]
      (.put q [:submit {:task-id "t1" :cf cf}])
      (#'shell/drain-stranded-submits! q)
      (is (.isCompletedExceptionally cf))
      (let [cause (try (.get cf) nil
                       (catch ExecutionException e (.getCause e)))]
        (is (= "supervisor stopped" (ex-message cause)))
        (is (= ::c/shutdown (:type (ex-data cause))))))))

(deftest print-method-is-compact-handle
  (testing "printing a future is a compact handle, not the supervisor guts (3.1)"
    (let [sup (c/create-supervisor {})]
      (try
        (let [f (c/submit sup {:prompt "x"} (fn [] (Thread/sleep 10000) :never))
              s (pr-str f)]
          (is (str/starts-with? s "#<"))
          (is (str/includes? s (str (:task-id f))))
          (is (not (str/includes? s ":tasks")))
          (is (not (str/includes? s "closure"))))
        (finally (c/stop! sup))))))

(deftest datafy-exposes-task-and-supervisor
  (testing "datafy of a stub yields the task map; nav :supervisor drills to state; datafy of sup is state (3.2/3.3)"
    (let [sup (c/create-supervisor {})]
      (try
        (let [f (c/submit sup {} (fn [] (Thread/sleep 10000) :never))]
          (is (wait-for #(some? (c/task f))) "task registered")
          (let [d (datafy/datafy f)]
            (is (= (:task-id f) (:task-id d)))
            (is (contains? d :status))
            (is (contains? d :realized?))
            (is (= (c/state sup) (datafy/nav d :supervisor (:supervisor d)))
                "nav :supervisor returns the full supervisor state")
            (is (= (c/state sup) (datafy/datafy sup)) "datafy of the supervisor is its state map")))
        (finally (c/stop! sup))))))

;; =============================================================================
;; Durable result memoization (dj.concurrency.store)
;; =============================================================================

(defn- spy-store
  "An atom-backed ResultStore plus the raw atom, so tests can both memoize and
   inspect what got persisted. Returns {:store ResultStore :data atom}."
  []
  (let [a (atom {})]
    {:store (reify store/ResultStore
              (lookup  [_ k]       (get @a k))
              (record! [_ k entry] (swap! a assoc k entry) nil)
              (evict!  [_ k]       (swap! a dissoc k) nil))
     :data  a}))

(deftest store-hit-short-circuits-closure
  (testing "a keyed hit resolves from the memo, never runs the closure, and is tagged :cached?"
    (let [{:keys [store]} (spy-store)
          sup (c/create-supervisor {:store store})
          k   [:sum "prompt-a"]]
      (try
        (is (= :computed @(c/submit sup {:dj.concurrency/durable-key k}
                                    (fn [] :computed))))
        (let [ran? (atom false)
              f2   (c/submit sup {:dj.concurrency/durable-key k}
                             (fn [] (reset! ran? true) (assert false)))]
          (is (= :computed (deref f2 2000 :timed-out)) "second submit returns the memo")
          (is (false? @ran?) "the second closure never ran")
          (is (= :resolved (:status (c/task f2))))
          (is (true? (:cached? (c/task f2))) "hit is tagged :cached?"))
        (finally (c/stop! sup))))))

(deftest store-miss-and-absent-config-behave-as-today
  (testing "miss / no-key / no-store each run the closure with no :cached? annotation"
    ;; miss: keyed + store, but empty store
    (let [{:keys [store]} (spy-store)
          sup (c/create-supervisor {:store store})]
      (try
        (let [f (c/submit sup {:dj.concurrency/durable-key [:x 1]}
                          (fn [] :fresh))]
          (is (= :fresh (deref f 2000 :timed-out)))
          (is (not (:cached? (c/task f)))))
        (finally (c/stop! sup))))
    ;; store present but task carries no key
    (let [{:keys [store data]} (spy-store)
          sup (c/create-supervisor {:store store})]
      (try
        (let [f (c/submit sup {} (fn [] :fresh))]
          (is (= :fresh (deref f 2000 :timed-out)))
          (is (not (:cached? (c/task f))))
          (is (empty? @data) "no key => nothing persisted"))
        (finally (c/stop! sup))))
    ;; no store at all, task carries a key
    (let [sup (c/create-supervisor {})]
      (try
        (let [f (c/submit sup {:dj.concurrency/durable-key [:x 1]}
                          (fn [] :fresh))]
          (is (= :fresh (deref f 2000 :timed-out)))
          (is (not (:cached? (c/task f)))))
        (finally (c/stop! sup))))))

(deftest store-caches-nil-and-false-via-envelope
  (testing "nil and false results are cached (envelope distinguishes a hit from a miss)"
    (doseq [v [nil false]]
      (let [{:keys [store]} (spy-store)
            sup (c/create-supervisor {:store store})
            k   [:v v]]
        (try
          (is (= v (deref (c/submit sup {:dj.concurrency/durable-key k} (fn [] v))
                          2000 :timed-out)))
          (let [f2 (c/submit sup {:dj.concurrency/durable-key k}
                             (fn [] (assert false)))]
            (is (= v (deref f2 2000 :timed-out)) (str "hit returns cached " (pr-str v)))
            (is (true? (:cached? (c/task f2)))))
          (finally (c/stop! sup)))))))

(deftest store-persists-before-future-resolves
  (testing "the future is not realized until record! returns (persist-then-publish)"
    (let [latch (CountDownLatch. 1)
          store (reify store/ResultStore
                  (lookup  [_ _] nil)
                  (record! [_ _ _] (.await latch) nil)   ;; block until released
                  (evict!  [_ _] nil))
          sup   (c/create-supervisor {:store store})]
      (try
        (let [f (c/submit sup {:dj.concurrency/durable-key [:slow 1]}
                          (fn [] :done))]
          (is (not (realized? f)) "not realized while record! blocks")
          (is (= :timed-out (deref f 300 :timed-out)) "still blocked mid-persist")
          (.countDown latch)
          (is (= :done (deref f 2000 :timed-out)) "resolves once durable"))
        (finally (c/stop! sup))))))

(deftest store-failures-degrade-to-no-cache
  (testing "a throwing lookup or record! never fails the task; both are logged"
    ;; lookup throws => task runs normally
    (let [entries (atom [])
          store   (reify store/ResultStore
                    (lookup  [_ _] (throw (ex-info "lookup boom" {})))
                    (record! [_ _ _] nil)
                    (evict!  [_ _] nil))
          sup     (c/create-supervisor {:store store :event-tap #(swap! entries conj %)})]
      (try
        (is (= :ok (deref (c/submit sup {:dj.concurrency/durable-key [:a 1]}
                                    (fn [] :ok))
                          2000 :timed-out)))
        (is (wait-for #(some (fn [e] (= :store-lookup-failed (:event e))) @entries))
            "a :store-lookup-failed entry was logged")
        (finally (c/stop! sup))))
    ;; record! throws => task still resolves with the computed value
    (let [entries (atom [])
          store   (reify store/ResultStore
                    (lookup  [_ _] nil)
                    (record! [_ _ _] (throw (ex-info "record boom" {})))
                    (evict!  [_ _] nil))
          sup     (c/create-supervisor {:store store :event-tap #(swap! entries conj %)})]
      (try
        (is (= :ok (deref (c/submit sup {:dj.concurrency/durable-key [:a 1]}
                                    (fn [] :ok))
                          2000 :timed-out)))
        (is (wait-for #(some (fn [e] (= :store-record-failed (:event e))) @entries))
            "a :store-record-failed entry was logged")
        (finally (c/stop! sup))))))

(deftest store-untouched-on-failure-path
  (testing "a keyed task that throws parks as before and writes nothing to the store"
    (let [{:keys [store data]} (spy-store)
          sup (c/create-supervisor {:store store})
          k   [:fails 1]]
      (try
        (let [f (c/submit sup {:dj.concurrency/durable-key k :dj.concurrency/max-attempts 1}
                          (fn [] (throw (ex-info "down" {}))))]
          (is (wait-for #(= :parked (status-of f))) "task parks after exhausting retries")
          (is (empty? @data) "a failed task records nothing"))
        (finally (c/stop! sup))))))

(deftest evict-forces-re-execution
  (testing "evict! removes the memo so the next keyed submit re-runs the closure"
    (let [{:keys [store]} (spy-store)
          sup (c/create-supervisor {:store store})
          k   [:e 1]
          runs (atom 0)]
      (try
        (is (= 1 (deref (c/submit sup {:dj.concurrency/durable-key k}
                                  (fn [] (swap! runs inc)))
                        2000 :timed-out)))
        ;; a hit would not re-run; prove eviction re-executes
        (is (= :evicted (c/evict! sup k)))
        (is (= 2 (deref (c/submit sup {:dj.concurrency/durable-key k}
                                  (fn [] (swap! runs inc)))
                        2000 :timed-out))
            "closure ran again after eviction")
        (finally (c/stop! sup))))
    (testing "evict! on a storeless supervisor is a no-op returning nil"
      (let [sup (c/create-supervisor {})]
        (try (is (nil? (c/evict! sup [:whatever])))
             (finally (c/stop! sup)))))))

(deftest deliver-result-is-never-memoized
  (testing "a REPL mock flows through resolve, not the worker, so it is not persisted"
    (let [{:keys [store data]} (spy-store)
          sup (c/create-supervisor {:store store})
          k   [:mock 1]
          ran (atom 0)]
      (try
        (let [f (c/submit sup {:dj.concurrency/durable-key k :dj.concurrency/max-attempts 1}
                          (fn [] (swap! ran inc) (throw (ex-info "down" {}))))]
          (is (wait-for #(= :parked (status-of f))))
          (c/deliver-result f :mocked)
          (is (= :mocked (deref f 2000 :timed-out)))
          (is (empty? @data) "the mock was not persisted")
          ;; resubmitting the same key must re-execute (miss), not return the mock
          (let [g (c/submit sup {:dj.concurrency/durable-key k}
                            (fn [] (swap! ran inc) :real))]
            (is (= :real (deref g 2000 :timed-out)) "closure ran; mock was never cached")))
        (finally (c/stop! sup))))))

(deftest atom-store-roundtrips
  (testing "atom-store lookup/record!/evict! obey the envelope contract"
    (let [s (store/atom-store)]
      (is (nil? (store/lookup s [:k])) "miss is nil")
      (store/record! s [:k] {:result 42})
      (is (= {:result 42} (store/lookup s [:k])) "hit returns the envelope")
      (store/record! s [:nilv] {:result nil})
      (is (= {:result nil} (store/lookup s [:nilv])) "a nil result is still a hit envelope")
      (store/evict! s [:k])
      (is (nil? (store/lookup s [:k])) "evicted key misses"))))
