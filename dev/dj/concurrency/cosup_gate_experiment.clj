(ns dj.concurrency.cosup-gate-experiment
  "U-COSUP-3 falsification experiment (RI 34, co-supervisor graduation).
   See ../../../agent/ledger/co_supervisor_graduation_reorientation_2026_07_06.md §3.

   QUESTION: the playground reference co-supervisor's F4 'serial single-flight'
   (wait for the backend to drain before each retry, via `backend-load`
   introspection) was written BEFORE the concurrency bound existed. Now that
   per-pool caps ship (RI 33), is that self-serialization REDUNDANT under a cap,
   but still NEEDED for an unbounded pool?

   METHOD: hold the co-supervisor fixed at its NAIVE extreme — poll parked tasks,
   retry every parked task immediately, NO idle-wait, NO cross-task
   serialization (only a per-task in-flight dedup so we don't spam one task's own
   retry). Vary ONE thing: whether the pool is CAPPED (W=1) or UNBOUNDED.

   Signal: PEAK backend backlog (concurrent worker jobs). If the cap paces the
   naive co-sup's retries, peak stays 1 and there is zero re-timeout waste. If the
   unbounded pool lets the retries burst, the F4 thundering herd recurs at the
   recovery layer (peak = N, heavy waste).

   Backend model (same as playground §4): one worker, service-ms per job. On a
   CLIENT timeout the caller abandons the wait but THE WORKER KEEPS THE JOB (real
   pile-up). `:fail-once` prompts throw a fast transient BEFORE reaching the worker
   the first time — a genuine failure the cap cannot prevent, so the co-sup has
   real parks to recover. Meter tracks concurrent worker jobs + peak."
  (:require [dj.concurrency :as c])
  (:import [java.util.concurrent Executors ExecutorService Future TimeUnit
            TimeoutException]))

;; ---------------------------------------------------------------------------
;; Single-worker backend with a peak-backlog meter
;; ---------------------------------------------------------------------------

(defn make-backend [{:keys [service-ms] :or {service-ms 200}}]
  {:exec       (Executors/newFixedThreadPool 1)
   :service-ms service-ms
   :fail-once  (atom #{})
   :meter      (atom {:submitted 0 :outstanding 0 :peak 0})})

(defn shutdown-backend! [b] (.shutdownNow ^ExecutorService (:exec b)))

(defn call
  "0-arg closure for `c/submit`. Fast transient on the first hit of a :fail-once
   prompt (never reaches the worker); otherwise runs on the single worker for
   service-ms with a client-side timeout. On timeout the worker keeps the job."
  [backend prompt {:keys [client-timeout-ms] :or {client-timeout-ms 350}}]
  (fn []
    (let [{:keys [^ExecutorService exec service-ms meter fail-once]} backend]
      (when (contains? @fail-once prompt)
        (swap! fail-once disj prompt)
        (throw (ex-info (str "flaky endpoint (fail-once): " prompt)
                        {:type :transient :prompt prompt})))
      (swap! meter (fn [m]
                     (let [o (inc (:outstanding m))]
                       (assoc m :submitted (inc (:submitted m))
                                :outstanding o
                                :peak (max (:peak m) o)))))
      (let [job ^Future (.submit exec ^Callable
                                 (fn []
                                   (Thread/sleep (long service-ms))
                                   (swap! meter #(update % :outstanding dec))
                                   (str "completion(" prompt ")")))]
        (try
          (.get job (long client-timeout-ms) TimeUnit/MILLISECONDS)
          (catch TimeoutException _
            ;; caller abandons; worker keeps draining the job (pile-up model)
            (throw (ex-info (str "backend timeout: " prompt)
                            {:type :timeout :prompt prompt}))))))))

;; ---------------------------------------------------------------------------
;; The NAIVE co-supervisor: no idle-wait, no cross-task serialization.
;; ---------------------------------------------------------------------------
;; Deliberately the OPPOSITE of the playground's F4 serial single-flight. It only
;; dedups a task against ITS OWN in-flight retry (so one park isn't spammed); it
;; NEVER waits for the backend to drain or for other tasks to settle. If anything
;; paces the retry storm, it is the pool cap, not this loop.

(def ^:private in-flight #{:queued :running :waiting-retry})

(defn start-naive-cosup
  [sup {:keys [poll-ms budget] :or {poll-ms 25 budget 8}}]
  (let [running (atom true)
        tries   (atom {})           ;; id -> retries issued
        pending (atom #{})]         ;; ids whose retry is in flight (own dedup)
    (Thread/startVirtualThread
     (fn []
       (while @running
         ;; drop from pending any task that has left the in-flight states
         ;; (re-parked, resolved, or gone) so it can be serviced again
         (swap! pending
                (fn [p] (into #{} (filter #(contains? in-flight
                                                      (:status (c/task sup %))))
                             p)))
         (doseq [{:keys [task-id]} (:tasks (c/explain-stuck sup))]
           (when-not (contains? @pending task-id)
             (let [n (get @tries task-id 0)]
               (if (>= n budget)
                 (do (c/abort sup task-id (ex-info "co-sup budget spent" {:type :gave-up}))
                     (swap! tries dissoc task-id))
                 (do (swap! tries update task-id (fnil inc 0))
                     (swap! pending conj task-id)
                     (c/retry sup task-id))))))         ;; fire — NO wait
         (Thread/sleep (long poll-ms)))))
    (fn stop [] (reset! running false))))

(defn start-serial-cosup
  "The OTHER half of the contract: a SERIALIZING co-sup for an UNBOUNDED pool.
   Services ONE park at a time — retry, then wait until that task SETTLES before
   taking the next. Crucially it settle-detects from TASK STATUS ONLY
   (`c/task` :status), never from backend introspection (tests U-COSUP-4: the
   playground's `backend-load`/`idle?` coupling is unnecessary). This is the
   pacing the pool cap gives you for free — here done by hand."
  [sup {:keys [poll-ms budget] :or {poll-ms 25 budget 8}}]
  (let [running (atom true)
        tries   (atom {})
        status  #(:status (c/task sup %))]
    (Thread/startVirtualThread
     (fn []
       (while @running
         (when-let [{:keys [task-id]} (first (:tasks (c/explain-stuck sup)))]
           (let [n (get @tries task-id 0)]
             (if (>= n budget)
               (do (c/abort sup task-id (ex-info "co-sup budget spent" {:type :gave-up}))
                   (swap! tries dissoc task-id))
               (do (swap! tries update task-id (fnil inc 0))
                   (c/retry sup task-id)
                   ;; serialize with STATUS ONLY (no backend introspection):
                   ;; (1) wait until the fire-and-forget retry is actually PICKED
                   ;; UP (task leaves :parked) — else the settle-wait races and
                   ;; returns instantly; (2) then wait until it settles again.
                   (while (and @running (= :parked (status task-id))) (Thread/sleep 5))
                   (while (and @running (contains? in-flight (status task-id))) (Thread/sleep 10))))))
         (Thread/sleep (long poll-ms)))))
    (fn stop [] (reset! running false))))

;; ---------------------------------------------------------------------------
;; Runner
;; ---------------------------------------------------------------------------

(defn- await-all [futs timeout-ms]
  (let [ps (mapv (fn [f]
                   (let [p (promise)]
                     (Thread/startVirtualThread
                      #(deliver p (try (deref f timeout-ms :TIMED-OUT)
                                       (catch Throwable t (str "ABORTED: " (ex-message t))))))
                     p))
                 futs)]
    (mapv deref ps)))

(defn- outcomes [results]
  (frequencies (map #(cond (= :TIMED-OUT %) :timed-out
                           (and (string? %) (.startsWith ^String % "ABORTED")) :aborted
                           :else :ok)
                    results)))

(defn run-case
  "Submit n fail-once tasks (max-attempts 1 => park on the first failure), let the
   naive co-sup recover them. `:cap` = W for the pool (int) or nil for unbounded.
   Returns a pure-data result map (peak backlog is the headline)."
  [{:keys [n cap service-ms client-timeout-ms budget label cosup]
    :or {n 6 service-ms 200 client-timeout-ms 350 budget 8 label "case" cosup :naive}}]
  (let [backend (make-backend {:service-ms service-ms})
        pool    :llm
        sup     (c/create-supervisor
                 (cond-> {:name label :event-tap (fn [_])}   ;; silent tap
                   cap (assoc :pool-caps {pool cap})))
        start   (if (= cosup :serial) start-serial-cosup start-naive-cosup)
        stop    (start sup {:budget budget})]
    (try
      (reset! (:fail-once backend) (set (map #(str "req-" %) (range n))))
      (let [futs (mapv (fn [i]
                         (let [prompt (str "req-" i)]
                           (c/submit sup
                                     {:prompt prompt
                                      :dj.concurrency/max-attempts 1
                                      :dj.concurrency/pool pool}
                                     (call backend prompt {:client-timeout-ms client-timeout-ms}))))
                       (range n))
            results (await-all futs 15000)
            m       @(:meter backend)]
        {:label            label
         :cosup            cosup
         :cap              (or cap :unbounded)
         :n                n
         :peak-backlog     (:peak m)
         :worker-jobs      (:submitted m)      ;; times a retry actually hit the worker
         :wasted-jobs      (- (:submitted m) n) ;; > 0 => retries re-piled and re-timed-out
         :outcomes         (outcomes results)})
      (finally (stop) (c/stop! sup) (shutdown-backend! backend)))))

(defn run-both
  "The A/B: identical naive co-sup, only the cap differs."
  []
  {:A-capped-W1   (run-case {:label "A/capped-W1"   :cap 1})
   :B-unbounded   (run-case {:label "B/unbounded"   :cap nil})})

(defn run-matrix
  "Full picture: pacing comes from EITHER the cap OR the co-sup's own
   serialization. A (cap + naive) and D (unbounded + serial) both stay at peak 1;
   B (unbounded + naive) storms; C (cap + serial) shows serialization is
   harmless-but-redundant once a cap exists."
  []
  {:A-cap+naive     (run-case {:label "A/cap+naive"     :cap 1   :cosup :naive})
   :B-unbounded+naive (run-case {:label "B/unbounded+naive" :cap nil :cosup :naive})
   :C-cap+serial    (run-case {:label "C/cap+serial"    :cap 1   :cosup :serial})
   :D-unbounded+serial (run-case {:label "D/unbounded+serial" :cap nil :cosup :serial})})
