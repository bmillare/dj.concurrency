(ns dj.concurrency.multi-resource-spike
  "SPIKE (dev-only, non-committal) — RI 18/19: is a ONE-supervisor / MANY-resources
   design coherent and clean enough to be in scope? See the scoping doc
   ../../../agent/ledger/multi_resource_supervisor_scoping_2026_07_06.md (§5 axes,
   §7 step 2 = this spike). Mirrors the RI-17 method (a falsifiable playground
   experiment built to try to BREAK the claim), but keeps EVERYTHING here — the
   shipped `dj.concurrency.policy` is untouched. This file commits to nothing; it
   only tells us whether the model holds together.

   The reframe: today one supervisor == one bounded resource (a single global W,
   a single global throttle). Here a task declares its RESOURCE via a context tag
   `:dj.concurrency/pool`, each pool `r` has its own cap `W_r`, and BOTH axes
   (concurrency admission AND the 429 throttle) partition per pool. `make-pooled-
   policy` below is a self-contained prototype of that policy; the reference policy
   is reused only for `classify-error` / `backoff`.

   Three falsifiable claims (from the scoping doc §7):
     (a) per-pool admission — each pool bounds INDEPENDENTLY; a saturated pool A
         does not throttle pool B's throughput (E7a).
     (b) the §3 throttle fix — a 429 tagged to pool A does NOT pause pool B. Run
         side-by-side with a GLOBAL-throttle control (the shipped reference policy)
         that DOES false-couple, to make the difference measurable (E7b).
     (c) F11 per-pool — a co-supervisor retry of a pool-A task re-acquires through
         pool A's OWN gate (no leak: peak A stays = W_a); pool B untouched (E7c).

   Run from the REPL:
     (require 'dj.concurrency.multi-resource-spike :reload)
     (dj.concurrency.multi-resource-spike/run-spike)"
  (:require [dj.concurrency :as c]
            [dj.concurrency.policy :as policy])
  (:import [java.util.concurrent Executors ExecutorService Future TimeUnit
            TimeoutException]))

;; =============================================================================
;; 0. Timeline (same shape as the playground's loud tap, kept self-contained)
;; =============================================================================

(def ^:private t0 (atom 0))
(def timeline (atom []))
(defn- reset-clock! [] (reset! t0 (System/currentTimeMillis)) (reset! timeline []))
(defn- rel [] (/ (- (System/currentTimeMillis) @t0) 1000.0))
(def ^:private print-lock (Object.))
(defn- emit! [src msg]
  (let [t (rel)]
    (swap! timeline conj {:t t :src src :msg msg})
    (locking print-lock
      (println (format "[+%06.2fs] %-9s %s" t src msg)))))
(defn- shorten [x]
  (cond
    (uuid? x) (subs (str x) 0 8)
    (map? x)  (into {} (map (fn [[k v]] [k (shorten v)])) x)
    :else     x))
(defn tap-fn
  "An `:event-tap` that renders the supervisor's events onto the shared timeline.
   Pool-aware events (`:admission-granted`, `:throttle-wait`, …) carry `:pool` in
   their data, so the timeline shows WHICH resource is saturated/throttled."
  [{:keys [level event data]}]
  (emit! "tap" (format "%-18s %-6s %s" event (or (some-> level name) "") (shorten data))))

;; =============================================================================
;; 1. The prototype PER-POOL policy (the thing under test)
;; =============================================================================
;; Generalizes the reference policy's TWO global constraints to per-pool:
;;   - admission: global `:max-in-flight W`   -> per-pool `:pool-caps {r W_r}`,
;;     in-flight counted PER POOL, drained per pool against its own budget.
;;   - throttle:  global `:throttle-expires-at` -> per-pool `:pool-throttle {r at}`,
;;     so a 429 tagged to pool r pauses ONLY pool r (the scoping-doc §3 fix).
;; A task's pool is a context tag; untagged => :default (unbounded unless capped).
;; Every worker-bound path (submit, timed retry, co-sup retry) routes to :queued
;; and is admitted at the SINGLE per-pool chokepoint `scan-pooled` (F11 per-pool).

(def ^:private terminal? #{:resolved :aborted :cancelled})
(defn- task-pool [t] (get-in t [:context :dj.concurrency/pool] :default))

(defn- apply-event
  [config [etype payload] state now]
  (let [task-id (:task-id payload)
        t       (get-in state [:tasks task-id])
        status  (:status t)]
    (case etype

      :submit
      (if (:shutdown? state)
        {:directives [[:abort {:task-id task-id
                               :error (ex-info "supervisor stopped"
                                               {:type :dj.concurrency/shutdown})}]]
         :state state}
        ;; Always enqueue; scan-pooled is the ONE per-pool admission chokepoint
        ;; (it admits in the same policy pass when a permit is free).
        (let [ctx' (assoc (:context payload) :dj.concurrency/attempts 1)]
          {:directives [[:log {:level :debug :event :submit-queued :task-id task-id
                               :data {:task-id task-id
                                      :pool (get ctx' :dj.concurrency/pool :default)}}]]
           :state (assoc-in state [:tasks task-id]
                            {:task-id task-id :status :queued :context ctx'
                             :closure (:closure payload)
                             :submitted-at (:submitted-at payload)})}))

      :success
      (cond
        (or (nil? t) (terminal? status))
        {:directives [[:log {:level :warn :event :late-success :task-id task-id :data task-id}]] :state state}
        (= :running status)
        {:directives [[:resolve {:task-id task-id :result (:result payload)}]]
         :state (assoc-in state [:tasks task-id :status] :resolved)}
        :else
        {:directives [[:log {:level :warn :event :illegal-transition :task-id task-id :data task-id}]] :state state})

      :failed
      (cond
        (or (nil? t) (terminal? status))
        {:directives [[:log {:level :warn :event :late-failure :task-id task-id :data task-id}]] :state state}
        (= :running status)
        (let [pool         (task-pool t)
              err-type     ((:classify-error config) (:error payload))
              attempts     (get-in t [:context :dj.concurrency/attempts] 1)
              max-attempts (get-in t [:context :dj.concurrency/max-attempts] (:max-attempts config))]
          (case err-type
            :fatal
            {:directives [[:abort {:task-id task-id :error (:error payload)}]]
             :state (update-in state [:tasks task-id] assoc :status :aborted :error (:error payload))}

            :rate-limited
            (let [window  (or (:retry-after (ex-data (:error payload))) (:default-throttle-ms config))
                  wake-at (+ now window)]
              ;; PER-POOL throttle: pause ONLY this pool (scoping-doc §3 fix).
              {:directives [[:log {:level :info :event :throttle-wait :task-id task-id
                                   :data {:task-id task-id :pool pool :wake-in-ms window}}]]
               :state (-> state
                          (update-in [:pool-throttle pool] (fnil max 0) wake-at)
                          (update-in [:tasks task-id] assoc
                                     :status :waiting-retry :wake-at wake-at
                                     :throttle? true :error (:error payload)))})

            :transient
            (if (< attempts max-attempts)
              (let [backoff ((:backoff-fn config) attempts)]
                {:directives [[:log {:level :debug :event :retry-scheduled :task-id task-id
                                     :data {:task-id task-id :pool pool
                                            :attempt (inc attempts) :wake-in-ms backoff}}]]
                 :state (-> state
                            (update-in [:tasks task-id] assoc
                                       :status :waiting-retry :wake-at (+ now backoff) :error (:error payload))
                            (update-in [:tasks task-id :context :dj.concurrency/attempts] (fnil inc 1)))})
              {:directives [[:log {:level :info :event :parked :task-id task-id
                                   :data {:task-id task-id :pool pool}}]]
               :state (update-in state [:tasks task-id] assoc :status :parked :error (:error payload))})))
        :else
        {:directives [[:log {:level :warn :event :illegal-transition :task-id task-id :data task-id}]] :state state})

      :tick
      {:directives [] :state state}

      :repl/retry
      (cond
        (#{:parked :waiting-retry :queued} status)
        ;; Fresh attempt budget, then re-enter THIS TASK'S POOL gate (F11 per-pool):
        ;; the pool tag rides on :context (preserved), so the retry re-acquires
        ;; through pool r's budget, never a global one and never uncounted.
        {:directives [[:log {:level :debug :event :retry-queued :task-id task-id
                             :data {:task-id task-id :pool (task-pool t)}}]]
         :state (update-in state [:tasks task-id] assoc
                           :status :queued :wake-at nil :throttle? nil
                           :context (assoc-in (:context t) [:dj.concurrency/attempts] 1))}
        (= :running status)
        {:directives [[:log {:level :warn :event :already-running :task-id task-id :data task-id}]] :state state}
        :else
        {:directives [[:log {:level :warn :event :no-such-task :task-id task-id :data task-id}]] :state state})

      :repl/abort
      (if (#{:parked :waiting-retry :queued :running} status)
        {:directives [[:abort {:task-id task-id :error (:error payload)}]]
         :state (update-in state [:tasks task-id] assoc :status :aborted :wake-at nil)}
        {:directives [[:log {:level :warn :event :invalid-abort :task-id task-id :data task-id}]] :state state})

      :shutdown
      (let [non-terminals (->> (:tasks state) vals (remove #(terminal? (:status %))))
            dirs (mapv (fn [task] [:abort {:task-id (:task-id task)
                                           :error (ex-info "supervisor stopped"
                                                           {:type :dj.concurrency/shutdown})}])
                       non-terminals)
            state' (reduce (fn [s task] (assoc-in s [:tasks (:task-id task) :status] :aborted))
                           state non-terminals)]
        {:directives dirs :state (assoc state' :shutdown? true)})

      {:directives [[:log {:level :error :event :unknown-event :data etype}]] :state state})))

(defn- scan-pooled
  "The per-pool admission chokepoint. Expire per-pool throttles, then for EACH
   pool independently: count that pool's in-flight (:running) tasks, drain its
   eligible (queued + due-retry) set oldest-first up to (W_r - in-flight). A pool
   with no cap is unbounded. Throttled pools admit nothing. Emits per-pool
   :admission-granted / :admission-wait (with :pool) so the tap shows which
   resource is saturated (scoping-doc U6)."
  [{:keys [directives state]} config now]
  (let [pool-caps (:pool-caps config)
        pt'       (into {} (remove (fn [[_ at]] (<= at now)) (:pool-throttle state)))
        state-1   (assoc state :pool-throttle pt')
        throttled? (fn [pool] (boolean (get pt' pool)))
        all       (vals (:tasks state-1))
        running-by-pool (frequencies (map task-pool (filter #(= :running (:status %)) all)))
        eligible  (filter (fn [t]
                            (and (not (throttled? (task-pool t)))
                                 (or (= :queued (:status t))
                                     (and (= :waiting-retry (:status t))
                                          (:wake-at t) (<= (:wake-at t) now)))))
                          all)
        by-pool   (into {} (map (fn [[p ts]] [p (sort-by :submitted-at ts)]))
                        (group-by task-pool eligible))
        drain     (reduce
                   (fn [acc [pool ts]]
                     (let [cap    (get pool-caps pool)               ;; nil = unbounded
                           inflt  (get running-by-pool pool 0)
                           budget (if cap (max 0 (- cap inflt)) (count ts))]
                       (-> acc
                           (update :admit into (take budget ts))
                           (update :block into (drop budget ts))
                           (assoc-in [:inflt pool] inflt)
                           (assoc-in [:cap pool] cap))))
                   {:admit [] :block [] :inflt {} :cap {}}
                   by-pool)
        admit     (:admit drain)
        block     (:block drain)
        exec-dirs (map (fn [t] [:execute (select-keys t [:task-id :context :closure])]) admit)
        grant-logs (map (fn [t]
                          (let [p (task-pool t)]
                            [:log {:level :debug :event :admission-granted :task-id (:task-id t)
                                   :data {:task-id (:task-id t) :pool p
                                          :in-flight (get-in drain [:inflt p]) :cap (get-in drain [:cap p])}}]))
                        admit)
        wait-logs (for [t block :when (not (:admission-waiting? t))]
                    (let [p (task-pool t)]
                      [:log {:level :info :event :admission-wait :task-id (:task-id t)
                             :data {:task-id (:task-id t) :pool p
                                    :in-flight (get-in drain [:inflt p]) :cap (get-in drain [:cap p])}}]))
        state-2 (as-> state-1 s
                  (reduce (fn [s t] (update-in s [:tasks (:task-id t)]
                                               assoc :status :running :wake-at nil
                                               :throttle? nil :admission-waiting? nil))
                          s admit)
                  (reduce (fn [s t] (assoc-in s [:tasks (:task-id t) :admission-waiting?] true))
                          s block))]
    {:directives (into [] (concat directives grant-logs wait-logs exec-dirs))
     :state state-2}))

(defn make-pooled-policy
  "Prototype per-pool policy. Opts: :pool-caps {pool W}, plus the reference
   tunables (:classify-error :backoff-fn :max-attempts :default-throttle-ms)."
  [opts]
  (let [config (merge {:classify-error policy/default-classify-error
                       :backoff-fn policy/default-backoff
                       :max-attempts 3 :default-throttle-ms 5000 :pool-caps {}}
                      (select-keys opts [:classify-error :backoff-fn :max-attempts
                                         :default-throttle-ms :pool-caps]))]
    (fn pooled-policy [event state]
      (let [now (:now (second event) (System/currentTimeMillis))]
        (-> (apply-event config event state now)
            (scan-pooled config now))))))

;; =============================================================================
;; 2. A multi-resource backend: one executor + meter PER POOL
;; =============================================================================

(defn make-pooled-backend
  "specs = {pool {:workers n :service-ms ms}}. Each pool gets its own fixed pool
   (the resource) and its own meter (so peak in-flight is measured per resource)."
  [specs]
  (into {}
        (map (fn [[pool {:keys [workers service-ms] :or {workers 1 service-ms 300}}]]
               [pool {:exec (Executors/newFixedThreadPool (int workers))
                      :workers workers :service-ms service-ms
                      :fail-once (atom #{}) :rate-limit-once (atom #{})
                      :meter (atom {:submitted 0 :completed 0 :outstanding 0 :peak 0})}]))
        specs))

(defn pool-load [backend pool] (:outstanding @(:meter (get backend pool))))
(defn- pool-peak [backend pool] (:peak @(:meter (get backend pool))))
(defn shutdown-pooled! [backend]
  (doseq [[_ b] backend] (.shutdownNow ^ExecutorService (:exec b))))

(defn- call
  "0-arg closure that hits `pool`'s resource with a client-side timeout. A prompt
   in that pool's :rate-limit-once throws a 429 once; in :fail-once throws a
   transient once."
  [backend pool prompt {:keys [client-timeout-ms] :or {client-timeout-ms 500}}]
  (fn []
    (let [{:keys [^ExecutorService exec service-ms meter fail-once rate-limit-once]} (get backend pool)]
      (when (contains? @rate-limit-once prompt)
        (swap! rate-limit-once disj prompt)
        (throw (ex-info (str "429 rate limited: " prompt)
                        {:status 429 :retry-after 300 :prompt prompt})))
      (when (contains? @fail-once prompt)
        (swap! fail-once disj prompt)
        (throw (ex-info (str "flaky endpoint: " prompt) {:type :transient :prompt prompt})))
      (swap! meter (fn [m] (let [o (inc (:outstanding m))]
                             (assoc m :submitted (inc (:submitted m)) :outstanding o
                                    :peak (max (:peak m) o)))))
      (let [job ^Future (.submit exec ^Callable
                                 (fn []
                                   (Thread/sleep (long service-ms))
                                   (swap! meter #(assoc % :completed (inc (:completed %))
                                                        :outstanding (dec (:outstanding %))))
                                   (str "completion(" prompt ")")))]
        (try
          (.get job (long client-timeout-ms) TimeUnit/MILLISECONDS)
          (catch TimeoutException _
            (throw (ex-info (str "backend timeout: " prompt) {:type :timeout :prompt prompt}))))))))

;; =============================================================================
;; 3. Helpers
;; =============================================================================

(defn- submit-task
  [sup backend pool prompt {:keys [max-attempts client-timeout-ms]
                            :or {max-attempts 2 client-timeout-ms 500}}]
  (c/submit sup
            {:prompt prompt :dj.concurrency/pool pool :dj.concurrency/max-attempts max-attempts}
            (call backend pool prompt {:client-timeout-ms client-timeout-ms})))

(defn- await-all [futs timeout-ms]
  (let [ps (mapv (fn [f]
                   (let [p (promise)]
                     (Thread/startVirtualThread
                      #(deliver p (try (deref f timeout-ms :TIMED-OUT)
                                       (catch Throwable t (str "ABORTED: " (ex-message t))))))
                     p))
                 futs)]
    (mapv deref ps)))

(defn- outcome-of [r]
  (cond (= :TIMED-OUT r) :TIMED-OUT
        (and (string? r) (.startsWith ^String r "ABORTED")) :aborted
        :else :ok))

(defn- start-pool-cosup
  "Minimal pool-scoped co-supervisor: reconciles against `(c/parked-tasks)` for
   tasks tagged `pool`, serialises retries (wait for the pool's resource to drain,
   grant one clean retry) with a per-task budget. Returns a stop fn."
  [sup backend pool {:keys [poll-ms max-retries] :or {poll-ms 30 max-retries 3}}]
  (let [running (atom true)
        tries   (atom {})
        idle?   #(zero? (pool-load backend pool))
        settled? #(not= :running (:status (c/task sup %)))]
    (Thread/startVirtualThread
     (fn []
       (while @running
         (when-let [[id t] (first (filter (fn [[_ t]] (= pool (get-in t [:context :dj.concurrency/pool])))
                                          (c/parked-tasks sup)))]
           (let [n (get @tries id 0)]
             (if (>= n max-retries)
               (do (emit! "co-sup" (str (shorten id) " budget spent -> abort"))
                   (c/abort sup id (or (:error t) (ex-info "co-sup gave up" {})))
                   (swap! tries dissoc id))
               (do (emit! "co-sup" (str "reconcile " (shorten id) " (try " (inc n) "/" max-retries
                                        ") pool=" pool " load=" (pool-load backend pool)))
                   (while (and @running (not (idle?))) (Thread/sleep 15))
                   (emit! "co-sup" (str (shorten id) " " pool " idle -> one clean retry"))
                   (swap! tries update id (fnil inc 0))
                   (c/retry sup id)
                   (Thread/sleep 50)
                   (while (and @running (not (settled? id))) (Thread/sleep 15))))))
         (Thread/sleep (long poll-ms)))))
    (fn [] (reset! running false))))

;; =============================================================================
;; 4. E7a — per-pool admission independence
;; =============================================================================

(defn scenario-pool-admission
  "E7a) One supervisor, two pools: A (1 worker, cap 1) and B (2 workers, cap 2).
   Submit `na` tasks to A and `nb` to B at once. CLAIM: each pool bounds
   INDEPENDENTLY — peak in-flight A == 1, peak B == 2 — and A's serial backlog
   does NOT hold up B (B completes at its own cap). Falsifies the fear that one
   supervisor forces one shared bound."
  [& {:keys [na nb] :or {na 4 nb 4}}]
  (println (format "\n========== E7a — per-pool admission (A cap1/1w, B cap2/2w) na=%d nb=%d ==========" na nb))
  (reset-clock!)
  (let [backend (make-pooled-backend {:llm-a {:workers 1 :service-ms 300}
                                      :llm-b {:workers 2 :service-ms 300}})
        sup (c/create-supervisor {:name "e7a" :event-tap tap-fn
                                  :policy (make-pooled-policy {:pool-caps {:llm-a 1 :llm-b 2}})})]
    (try
      (let [futs    (into (mapv #(submit-task sup backend :llm-a (str "a-" %) {:max-attempts 2}) (range na))
                          (mapv #(submit-task sup backend :llm-b (str "b-" %) {:max-attempts 2}) (range nb)))
            results (await-all futs 15000)]
        (emit! "REPORT" (format "E7a outcomes=%s  peak A=%d (cap 1)  peak B=%d (cap 2)"
                                (frequencies (map outcome-of results))
                                (pool-peak backend :llm-a) (pool-peak backend :llm-b)))
        results)
      (finally (c/stop! sup) (shutdown-pooled! backend)))))

;; =============================================================================
;; 5. E7b — per-pool throttle vs. a GLOBAL-throttle control (the §3 fix)
;; =============================================================================

(defn scenario-throttle-isolation
  "E7b) A 429 hits pool A. Does pool B keep running? Sequence: submit A's 429 task,
   let it fail-fast and set A's throttle, THEN submit a B task and time how long B
   takes to complete.
     :mode :per-pool => prototype pooled policy. B unaffected (~ its own service).
     :mode :global   => the SHIPPED reference policy (one global throttle). B is
                        false-coupled: it waits out A's window before it can run.
   The gap between the two B-latencies is the bug the per-pool design fixes."
  [& {:keys [mode] :or {mode :per-pool}}]
  (println (format "\n========== E7b — throttle isolation (mode=%s) ==========" (name mode)))
  (reset-clock!)
  (let [backend (make-pooled-backend {:llm-a {:workers 1 :service-ms 300}
                                      :llm-b {:workers 1 :service-ms 300}})
        _ (reset! (:rate-limit-once (:llm-a backend)) #{"a-0"})
        policy (if (= mode :per-pool)
                 (make-pooled-policy {:pool-caps {:llm-a 1 :llm-b 1}})
                 (policy/make-reference-policy {}))    ;; global throttle control
        sup (c/create-supervisor {:name (str "e7b-" (name mode)) :event-tap tap-fn :policy policy})]
    (try
      (let [fa (submit-task sup backend :llm-a "a-0" {:max-attempts 3})
            _  (Thread/sleep 80)                        ;; a-0 429s fast -> throttle set
            t-b (System/currentTimeMillis)
            fb (submit-task sup backend :llm-b "b-0" {:max-attempts 3})
            rb (deref fb 8000 :TIMED-OUT)
            b-latency (- (System/currentTimeMillis) t-b)
            ra (deref fa 8000 :TIMED-OUT)]
        (emit! "REPORT" (format "E7b/%s  B-latency=%dms (B=%s, A=%s)  [A pool threw the 429]"
                                (name mode) b-latency (outcome-of rb) (outcome-of ra)))
        {:mode mode :b-latency b-latency :b (outcome-of rb) :a (outcome-of ra)})
      (finally (c/stop! sup) (shutdown-pooled! backend)))))

;; =============================================================================
;; 6. E7c — F11 per-pool: a co-sup retry re-acquires through its OWN pool's gate
;; =============================================================================

(defn scenario-cosup-reacquire
  "E7c) Pool A (cap 1): task a-1 hits a genuine failure and PARKS (max-attempts 1);
   a pool-A co-supervisor services it. Pool B (cap 1) runs normally. CLAIM (F11
   per-pool): the co-sup retry re-enters pool A's gate (tap shows :retry-queued ->
   :admission-granted {:pool :llm-a}), so peak A stays == 1 (NO leak); pool B is
   untouched (peak B == 1). Falsifies the worry that cross-resource retries leak a
   pool's bound."
  [& {:keys [na nb] :or {na 3 nb 3}}]
  (println (format "\n========== E7c — F11 per-pool (co-sup re-acquire, A cap1 / B cap1) ==========" ))
  (reset-clock!)
  (let [backend (make-pooled-backend {:llm-a {:workers 1 :service-ms 300}
                                      :llm-b {:workers 1 :service-ms 300}})
        _ (reset! (:fail-once (:llm-a backend)) #{"a-1"})
        sup (c/create-supervisor {:name "e7c" :event-tap tap-fn
                                  :policy (make-pooled-policy {:pool-caps {:llm-a 1 :llm-b 1}})})
        stop-cosup (start-pool-cosup sup backend :llm-a {:max-retries 3})]
    (try
      (let [futs    (into (mapv #(submit-task sup backend :llm-a (str "a-" %) {:max-attempts 1}) (range na))
                          (mapv #(submit-task sup backend :llm-b (str "b-" %) {:max-attempts 1}) (range nb)))
            results (await-all futs 15000)]
        (emit! "REPORT" (format "E7c outcomes=%s  peak A=%d (cap 1, no leak?)  peak B=%d (cap 1)"
                                (frequencies (map outcome-of results))
                                (pool-peak backend :llm-a) (pool-peak backend :llm-b)))
        results)
      (finally (stop-cosup) (c/stop! sup) (shutdown-pooled! backend)))))

;; =============================================================================
;; 7. Driver
;; =============================================================================

(defn run-spike
  "RI 18/19 multi-resource spike. Read each REPORT + timeline:
   E7a: peak A==1 AND peak B==2 (pools bound independently, A doesn't hold up B).
   E7b: per-pool B-latency ~= its own service; global B-latency ~= service + A's
        leftover throttle window (the false-coupling the design removes).
   E7c: peak A stays ==1 through the co-sup retry (no leak); B peak ==1."
  [& _]
  (scenario-pool-admission)
  (scenario-throttle-isolation :mode :per-pool)
  (scenario-throttle-isolation :mode :global)      ;; control: shows the §3 bug
  (scenario-cosup-reacquire)
  (println "\n========== spike done — compare REPORT lines (peaks & B-latencies) ==========")
  :done)

(comment
  (require 'dj.concurrency.multi-resource-spike :reload)
  (dj.concurrency.multi-resource-spike/run-spike)
  ;; individual levers:
  (scenario-pool-admission :na 5 :nb 5)
  (scenario-throttle-isolation :mode :per-pool)
  (scenario-throttle-isolation :mode :global)
  (scenario-cosup-reacquire)
  )
