(ns dj.concurrency.reference-co-supervisor-test
  "Tests for the graduated `dev/` reference co-supervisor
   (`dj.concurrency.reference-co-supervisor`). These prove the graduation claims
   from the RI-34/RI-35 arc:

   - F5  : an unattended run recovers its parked tasks (the co-sup IS the
           intervener when no human is watching).
   - F4  : a task that never recovers escalates after its per-task budget
           (pluggable `:on-exhausted`), instead of retrying forever.
   - F8  : composed with a pool cap, recovery never exceeds the cap (peak == W)
           and leaves no permit leaked (F13/F14).
   - E7c : `:pool`-scoped recovery acts only on the chosen pool.
   - cap-or-serialize : the `:serialize? true` fallback recovers an UNBOUNDED
           pool at peak 1, using task status alone (no backend introspection)."
  (:require [clojure.test :refer [deftest testing is]]
            [dj.concurrency :as c]
            [dj.concurrency.reference-co-supervisor :as co]))

;; =============================================================================
;; Helpers
;; =============================================================================

(defn- wait-for
  ([pred] (wait-for pred 3000))
  ([pred timeout-ms]
   (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
     (loop []
       (cond
         (pred) true
         (> (System/currentTimeMillis) deadline) false
         :else (do (Thread/sleep 10) (recur)))))))

(defn- recovering-backend
  "A backend whose per-task closures throw a transient on their FIRST call (so a
   task submitted with `:max-attempts 1` PARKS on the first failure) and SUCCEED
   on any later call (the co-sup's retry). Tracks peak concurrent successful
   executions so a test can assert recovery stayed within a cap.

   Returns {:closure (fn [id] -> 0-arg closure) :peak atom :calls atom}."
  [service-ms]
  (let [cur   (atom 0)
        peak  (atom 0)
        calls (atom {})]
    {:peak  peak
     :calls calls
     :closure
     (fn [id]
       (fn []
         (let [n (get (swap! calls update id (fnil inc 0)) id)]
           (if (= 1 n)
             (throw (ex-info (str "flaky " id) {:type :transient}))
             (do (swap! cur inc)
                 (swap! peak max @cur)
                 (Thread/sleep (long service-ms))
                 (swap! cur dec)
                 :ok)))))}))

(defn- always-fails
  "A closure that always throws a transient — a task that will never recover, so
   the co-sup must escalate on budget. Counts how many times it ran."
  [calls]
  (fn [] (swap! calls inc) (throw (ex-info "permanently down" {:type :transient}))))

;; =============================================================================
;; F5 — an unattended run recovers its parked tasks (headline recipe)
;; =============================================================================

(deftest naive-cosup-under-cap-recovers-parked-tasks
  (testing "cap + naive co-sup: N fail-once tasks all park, then all recover unattended"
    (let [{:keys [closure calls]} (recovering-backend 20)
          sup  (c/create-supervisor {:pool-caps {:llm 2}})
          stop (co/start-co-supervisor sup {:pool :llm :budget 5 :poll-ms 25})]
      (try
        (let [fs (mapv (fn [i]
                         (c/submit sup {:dj.concurrency/pool :llm
                                        :dj.concurrency/max-attempts 1}
                                   (closure (str "req-" i))))
                       (range 6))]
          (doseq [f fs]
            (is (= :ok (deref f 5000 :timed-out)) "every parked task is recovered by the co-sup"))
          (is (empty? (c/parked-tasks sup)) "nothing left parked after recovery")
          (is (every? #(= 2 (get @calls %)) (map #(str "req-" %) (range 6)))
              "each task ran exactly twice: one park + one clean retry"))
        (finally (stop) (c/stop! sup))))))

;; =============================================================================
;; F4 — a never-recovering task escalates after its budget (pluggable)
;; =============================================================================

(deftest cosup-escalates-on-budget-with-pluggable-hook
  (testing "a task that never recovers escalates via :on-exhausted after `budget` retries"
    (let [calls     (atom 0)
          exhausted (atom nil)
          sup       (c/create-supervisor {:pool-caps {:llm 1}})
          ;; :on-exhausted delivers a fallback instead of aborting — proving the
          ;; hook can implement ANY escalation, and that it receives pure data.
          stop      (co/start-co-supervisor
                     sup {:pool :llm :budget 2 :poll-ms 25
                          :on-exhausted (fn [s id summary]
                                          (reset! exhausted summary)
                                          (c/deliver-result s id :fallback))})]
      (try
        (let [f (c/submit sup {:dj.concurrency/pool :llm
                               :dj.concurrency/max-attempts 1}
                          (always-fails calls))]
          (is (= :fallback (deref f 5000 :timed-out))
              ":on-exhausted delivered a fallback once the budget was spent")
          (is (wait-for #(some? @exhausted)))
          (let [s @exhausted]
            (is (= :transient (:error-type s)) "escalation hook gets the pure-data summary")
            (is (string? (:error s)) ":error is a message string, not a Throwable")
            (is (= :llm (:pool s))))
          ;; initial attempt + exactly `budget` co-sup retries = 3 runs, then it stopped.
          (is (= 3 @calls) "ran initial + budget(2) retries, then escalated (no infinite loop)"))
        (finally (stop) (c/stop! sup))))))

;; =============================================================================
;; F8 / F13 / F14 — recovery composes with the cap: peak == W, no permit leak
;; =============================================================================

(deftest naive-cosup-recovery-never-exceeds-the-cap
  (testing "under cap W=1 the naive co-sup's recovery holds peak backend load at 1 and leaks no permit"
    (let [{:keys [closure peak]} (recovering-backend 30)
          sup  (c/create-supervisor {:pool-caps {:llm 1}})
          stop (co/start-co-supervisor sup {:pool :llm :budget 5 :poll-ms 25})]
      (try
        (let [fs (mapv (fn [i]
                         (c/submit sup {:dj.concurrency/pool :llm
                                        :dj.concurrency/max-attempts 1}
                                   (closure (str "req-" i))))
                       (range 6))]
          (doseq [f fs] (is (= :ok (deref f 6000 :timed-out))))
          (is (= 1 @peak) "recovery never re-piled the backend beyond the cap")
          ;; no permit leak: a fresh task on the same pool still admits and runs.
          (is (= :done (deref (c/submit sup {:dj.concurrency/pool :llm} (fn [] :done))
                              3000 :timed-out))
              "the pool's permit was released by every recovery; a new task runs"))
        (finally (stop) (c/stop! sup))))))

;; =============================================================================
;; E7c — :pool-scoped recovery acts only on the chosen pool
;; =============================================================================

(deftest cosup-pool-filter-recovers-only-its-pool
  (testing ":pool :a co-sup recovers pool A's park and leaves pool B's park untouched"
    (let [{:keys [closure]} (recovering-backend 10)
          sup   (c/create-supervisor {:pool-caps {:a 1 :b 1}})
          stop-a (co/start-co-supervisor sup {:pool :a :budget 5 :poll-ms 25})]
      (try
        (let [fa (c/submit sup {:dj.concurrency/pool :a :dj.concurrency/max-attempts 1}
                           (closure "a"))
              fb (c/submit sup {:dj.concurrency/pool :b :dj.concurrency/max-attempts 1}
                           (closure "b"))]
          (is (= :ok (deref fa 5000 :timed-out)) "pool A's task is recovered")
          (is (wait-for #(seq (c/parked-tasks sup))) "pool B's task is still parked")
          (is (= :b (get-in (first (:tasks (c/explain-stuck sup))) [:pool]))
              "the only remaining park is pool B's — the :a co-sup never touched it")
          ;; a second co-sup scoped to :b clears it, proving B was merely un-serviced.
          (let [stop-b (co/start-co-supervisor sup {:pool :b :budget 5 :poll-ms 25})]
            (try (is (= :ok (deref fb 5000 :timed-out)) "a :b co-sup recovers pool B")
                 (finally (stop-b)))))
        (finally (stop-a) (c/stop! sup))))))

;; =============================================================================
;; cap-or-serialize — the serial fallback recovers an UNBOUNDED pool at peak 1
;; =============================================================================

(deftest serial-cosup-recovers-unbounded-pool-at-peak-one
  (testing ":serialize? true paces recovery of an uncapped pool using task status alone"
    (let [{:keys [closure peak]} (recovering-backend 40)
          ;; no :pool-caps -> the :default pool is UNBOUNDED. A naive loop would
          ;; storm here; serial mode must hold peak at 1 with no backend meter.
          sup  (c/create-supervisor {})
          stop (co/start-co-supervisor sup {:serialize? true :budget 5 :poll-ms 25})]
      (try
        (let [fs (mapv (fn [i]
                         (c/submit sup {:dj.concurrency/max-attempts 1}
                                   (closure (str "req-" i))))
                       (range 4))]
          (doseq [f fs] (is (= :ok (deref f 8000 :timed-out)) "each task recovers"))
          (is (= 1 @peak) "serial single-flight held peak backend load at 1 with no cap"))
        (finally (stop) (c/stop! sup))))))
