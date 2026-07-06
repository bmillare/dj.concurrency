(ns dj.concurrency.playground
  "PLAYGROUND (dev-only, non-committal) — the learning vehicle for the park /
   supervision work. See ../../../agent/ledger/park_supervision_design_2026_07_06.md
   (§12.4 tap reframing, §12.2 reference co-supervisor, §12.3 the local-worker
   lesson). Nothing here is shipped API; it exists to make in-flight supervisor
   behavior LEGIBLE so we can *discover* the workload-specific policy a saturated
   single-worker backend needs — instead of learning it by hanging in production.

   The motivating, unintuitive lesson we want a consumer to reach in MINUTES:

     For a local / single-worker LLM, a TIMEOUT does not mean 'retry soon'. It
     means the one worker is saturated. Retrying fast piles MORE load onto the
     already-overloaded worker and makes it worse. Slow => be patient
     (self-throttle / bound concurrency), the OPPOSITE of the external-provider
     instinct.

   Run the whole narrative from the REPL:

     (require 'dj.concurrency.playground :reload)
     (dj.concurrency.playground/run-all)

   Everything is deterministic-ish (fixed sleeps, no randomness) so the event
   timeline reads the same way each run.

   deref contract at point of use (Phase-1 SP3 docs, prototyped here as a note):
     - (deref f)                 blocks until the task reaches a TERMINAL state
                                 (:resolved -> value ; :aborted -> throws).
                                 A :parked task NEVER becomes terminal on its own,
                                 so a bare @f on a parked task blocks FOREVER.
                                 => Always use (deref f timeout-ms timeout-val),
                                    or run a co-supervisor that services parks."
  (:require [dj.concurrency :as c]
            [dj.concurrency.policy :as policy])
  (:import [java.util.concurrent Executors ExecutorService Future TimeUnit
            TimeoutException Semaphore]))

;; =============================================================================
;; 0. A live, human-readable timeline (the "loud tap" default subscriber, V-A)
;; =============================================================================
;; One lock-guarded println channel that INTERLEAVES three sources into a single
;; wall-clock timeline: the supervisor's tap (:log-fn), the backend load meter,
;; and the co-supervisor. Reading top-to-bottom tells the whole story.

(def ^:private t0 (atom 0))
(def timeline
  "Every emit! is teed here as {:t :src :msg} so a run can be inspected
   programmatically even when the live prints land on a virtual thread whose
   *out* isn't the caller's (e.g. driven over nREPL). Reset per scenario."
  (atom []))
(defn- reset-clock! [] (reset! t0 (System/currentTimeMillis)) (reset! timeline []))
(defn- rel [] (/ (- (System/currentTimeMillis) @t0) 1000.0))

(def ^:private print-lock (Object.))
(defn- emit! [src msg]
  (let [t (rel)]
    (swap! timeline conj {:t t :src src :msg msg})
    (locking print-lock
      (println (format "[+%06.2fs] %-9s %s" t src msg)))))

(defn dump-timeline
  "Reprint the captured timeline of the last scenario (for post-hoc inspection)."
  []
  (doseq [{:keys [t src msg]} @timeline]
    (println (format "[+%06.2fs] %-9s %s" t src msg)))
  (count @timeline))

(defn- shorten
  "Compact uuids to 8 chars so the timeline stays readable; recurse into maps."
  [x]
  (cond
    (uuid? x) (subs (str x) 0 8)
    (map? x)  (into {} (map (fn [[k v]] [k (shorten v)])) x)
    :else     x))

;; =============================================================================
;; 1. The reframed tap as a LOUD default subscriber (Phase-1 SP1/V-A)
;; =============================================================================

(defn timeline-log-fn
  "A :log-fn that renders the supervisor's event tap live. This is the 'make the
   silent default loud' relief: today :log-fn defaults to `tap>`, which is a no-op
   unless something is registered — so a run parks in total silence. Here every
   lifecycle event is printed the instant it is emitted."
  [{:keys [level event data]}]
  (emit! "tap" (format "%-16s %-6s %s" event (or (some-> level name) "") (shorten data))))

;; =============================================================================
;; 2. (GRADUATED) surfacing the silent transitions
;; =============================================================================
;; This playground originally wrapped the policy to surface the transitions the
;; reference policy was SILENT on (F4 transient backoff, F3 429 throttle-wait) —
;; the invisibility that hid the retry-storm (finding F1). That fix has since been
;; GRADUATED into the reference policy itself: F4 now emits :retry-scheduled
;; (:debug, {:data {:task-id :attempt :max-attempts :wake-in-ms}}) and F3 emits
;; :throttle-wait (:info, {:data {:task-id :wake-in-ms}}). So the scenarios below
;; use the plain reference policy and the tap tells the whole story on its own.

;; =============================================================================
;; 3. explain-stuck — print-friendly view over parked-tasks (Phase-1 SP1/V-C)
;; =============================================================================

(defn explain-stuck
  "Human-readable dump of (c/parked-tasks sup): the AUTHORITATIVE todo list a
   co-supervisor reconciles against. The tap is a doorbell (lossy, low-latency);
   THIS is the source of truth (never lossy). Returns the parked count."
  [sup]
  (let [parked (c/parked-tasks sup)]
    (if (empty? parked)
      (println "  (no parked tasks)")
      (doseq [[id t] parked]
        (println (format "  PARKED %s  attempts=%s  error=%s"
                         (shorten id)
                         (get-in t [:context :dj.concurrency/attempts])
                         (some-> (:error t) ex-message)))
        (println (format "         context=%s" (pr-str (dissoc (:context t)
                                                               :dj.concurrency/attempts))))))
    (count parked)))

;; =============================================================================
;; 4. Simulated slow backend (deterministic, no infra); default = ONE worker
;; =============================================================================
;; A fixed thread pool of `:workers` (default 1) IS the backend: it serializes
;; requests when workers=1, runs up to W concurrently otherwise. The client call
;; waits only up to `client-timeout-ms`; on timeout it abandons the wait BUT THE
;; WORKER KEEPS THE JOB (models real pile-up — an abandoned request still consumes
;; a worker, and a retry adds ANOTHER job behind it). The meter tracks worker
;; backlog (submitted - completed) so pile-up is measurable.
;;
;; `:fail-once` is a set of prompts that FAIL FAST the first time they are called
;; (a flaky endpoint) — models a genuine transient ERROR the concurrency bound
;; cannot prevent (it isn't saturation), so the co-supervisor has something real
;; to recover in E3. The failure is fast and never reaches a worker.
;;
;; `:rate-limit-once` is like `:fail-once` but throws a REAL 429 (with retry-after)
;; the first time — the reference policy throttles the WHOLE supervisor for the
;; window (§3). Used by E4 to see whether the concurrency bound fights or composes
;; with the supervisor-wide throttle.

(defn make-backend
  [{:keys [service-ms workers] :or {service-ms 300 workers 1}}]
  {:exec            (Executors/newFixedThreadPool (int workers))
   :workers         workers
   :service-ms      service-ms
   :fail-once       (atom #{})
   :rate-limit-once (atom #{})
   :meter           (atom {:submitted 0 :completed 0 :outstanding 0 :peak 0})})

(defn backend-load [backend] (:outstanding @(:meter backend)))
(defn shutdown-backend! [backend] (.shutdownNow ^ExecutorService (:exec backend)))

(defn call
  "Returns a 0-arg closure (hand to `c/submit`) that hits the backend with a
   client-side timeout. Timeout throws an ex-info tagged {:type :timeout}; a
   prompt in the backend's :fail-once set throws {:type :transient} the first
   time (then clears itself). The default classifier sees both as :transient."
  [backend prompt {:keys [client-timeout-ms] :or {client-timeout-ms 500}}]
  (fn []
    (let [{:keys [^ExecutorService exec service-ms meter fail-once rate-limit-once]} backend]
      (when (and rate-limit-once (contains? @rate-limit-once prompt))
        (swap! rate-limit-once disj prompt)
        (throw (ex-info (str "429 rate limited: " prompt)
                        {:status 429 :retry-after 300 :prompt prompt})))
      (when (and fail-once (contains? @fail-once prompt))
        (swap! fail-once disj prompt)
        (throw (ex-info (str "flaky endpoint (fail-once): " prompt)
                        {:type :transient :prompt prompt})))
      (swap! meter (fn [m]
                     (let [o (inc (:outstanding m))]
                       (assoc m :submitted (inc (:submitted m))
                                :outstanding o
                                :peak (max (:peak m) o)))))
      (let [job ^Future
            (.submit exec ^Callable
                     (fn []
                       (Thread/sleep (long service-ms))
                       (swap! meter #(assoc % :completed (inc (:completed %))
                                              :outstanding (dec (:outstanding %))))
                       (str "completion(" prompt ")")))]
        (try
          (.get job (long client-timeout-ms) TimeUnit/MILLISECONDS)
          (catch TimeoutException _
            (throw (ex-info (str "backend timeout: " prompt)
                            {:type :timeout :prompt prompt}))))))))

;; =============================================================================
;; 5. The reference co-supervisor (Phase-2 deliverable, §12.2)
;; =============================================================================
;; Does NOT trust the lossy tap as a work queue. It RECONCILES against
;; (c/parked-tasks) — authoritative state, never lossy — and SERVICES parks
;; SERIALLY (single-flight): wait for the worker to drain, grant one clean retry,
;; wait for it to settle, then take the next park. This is the mechanism that
;; collapses the "no-intervener" worst case (§12.2): an unattended job runs the
;; co-supervisor as its intervener.
;;
;; FINDING (why serial): the first version spawned a waiter per park, each
;; retrying the instant the worker went idle. All parked tasks then fired at once
;; and RE-PILED the single worker — the scenario-A thundering herd, reborn at the
;; RECOVERY layer. So the concurrency-limit lesson (scenario C) recurs here: a
;; correct co-supervisor for a single-worker backend must serialize its own
;; retries, not just wait for idle. A per-task retry BUDGET escalates (abort) a
;; task that will not settle, so recovery can't loop forever.

(defn start-co-supervisor
  "Reference co-supervisor. Reconciles against (c/parked-tasks) and services one
   park at a time. Options: :poll-ms, :max-retries (co-sup-level budget per task),
   :on-exhausted (fn [sup id task]) run when the budget is spent (default: abort).
   Returns a 0-arg stop fn."
  [sup backend {:keys [poll-ms max-retries on-exhausted]
                :or {poll-ms 30 max-retries 3}}]
  (let [running (atom true)
        tries   (atom {})
        idle?   #(zero? (backend-load backend))
        settled? #(not= :running (:status (c/task sup %)))]
    (Thread/startVirtualThread
     (fn []
       (while @running
         (when-let [[id t] (first (c/parked-tasks sup))]
           (let [n (get @tries id 0)]
             (if (>= n max-retries)
               (do (emit! "co-sup" (str (shorten id) " co-sup budget spent -> "
                                        (if on-exhausted "escalate" "abort")))
                   (if on-exhausted
                     (on-exhausted sup id t)
                     (c/abort sup id (or (:error t) (ex-info "co-sup gave up" {}))))
                   (swap! tries dissoc id))
               (do
                 (emit! "co-sup" (str "reconcile park " (shorten id)
                                      " (try " (inc n) "/" max-retries "), load="
                                      (backend-load backend)))
                 ;; local-worker lesson AS behavior: saturated worker => be patient
                 (while (and @running (not (idle?))) (Thread/sleep 15))
                 (emit! "co-sup" (str (shorten id) " worker idle -> one clean retry"))
                 (swap! tries update id (fnil inc 0))
                 (c/retry sup id)
                 (Thread/sleep 50)                       ;; let the shell pick up the retry
                 (while (and @running (not (settled? id))) (Thread/sleep 15))))))
         (Thread/sleep (long poll-ms)))))
    (fn stop-co-supervisor [] (reset! running false))))

;; =============================================================================
;; 6. Scenario helpers
;; =============================================================================

(defn- submit-batch
  [sup backend n {:keys [max-attempts client-timeout-ms]
                  :or {max-attempts 2 client-timeout-ms 500}}]
  (mapv (fn [i]
          (let [p (str "req-" i)]
            (c/submit sup
                      {:prompt p :dj.concurrency/max-attempts max-attempts}
                      (call backend p {:client-timeout-ms client-timeout-ms}))))
        (range n)))

(defn- await-all
  "Deref every future CONCURRENTLY (one vthread each) so parked tasks that never
   resolve don't serialize their timeouts into minutes of wall time."
  [futs timeout-ms]
  (let [ps (mapv (fn [f]
                   (let [p (promise)]
                     (Thread/startVirtualThread
                      #(deliver p (try (deref f timeout-ms :TIMED-OUT)
                                       (catch Throwable t (str "ABORTED: " (ex-message t))))))
                     p))
                 futs)]
    (mapv deref ps)))

(defn- outcome [results]
  (frequencies (map #(cond (= :TIMED-OUT %) :TIMED-OUT
                           (and (string? %) (.startsWith ^String % "ABORTED")) :aborted
                           :else :ok)
                    results)))

(defn- report [label backend results]
  (let [m @(:meter backend)
        n (count results)]
    (emit! "REPORT" (format "%s: outcomes=%s" label (outcome results)))
    (emit! "REPORT" (format "%s: %d logical reqs -> %d worker jobs (%d WASTED by retries), peak backlog=%d"
                            label n (:submitted m) (- (:submitted m) n) (:peak m)))))

(defn- gated-run
  "Admit each of `n` requests through a shared `sem` (acquire -> submit -> deref ->
   release), one vthread per request. The semaphore is the CONCURRENCY-LIMIT
   primitive: a request only reaches the backend once a permit is free, so peak
   in-flight is bounded by (.availablePermits). Returns the results vector.
   `first-req` offsets the request numbering so two consumers can share a backend
   without colliding prompt names (used by E2)."
  [sup backend sem n {:keys [client-timeout-ms max-attempts first-req label]
                      :or {client-timeout-ms 500 max-attempts 2 first-req 0 label "gate"}}]
  (let [ps (mapv (fn [i]
                   (let [idx (+ first-req i)
                         p   (promise)]
                     (Thread/startVirtualThread
                      (fn []
                        (.acquire ^Semaphore sem)
                        (emit! label (str "req-" idx " admitted (permits left "
                                          (.availablePermits ^Semaphore sem) ")"))
                        (let [prompt (str "req-" idx)
                              f (c/submit sup {:prompt prompt
                                               :dj.concurrency/max-attempts max-attempts}
                                          (call backend prompt {:client-timeout-ms client-timeout-ms}))]
                          (deliver p (try (deref f 20000 :TIMED-OUT)
                                          (catch Throwable t (str "ABORTED: " (ex-message t)))))
                          (.release ^Semaphore sem))))
                     p))
                 (range n))]
    (mapv deref ps)))

;; =============================================================================
;; 7. Scenarios
;; =============================================================================

(defn scenario-naive
  "A) NAIVE: default classifier treats a timeout as :transient, so a saturated
   single worker gets a RETRY-STORM piled on top of it. Watch worker backlog and
   wasted jobs climb. (No co-supervisor: any task that exhausts retries PARKS and
   its consumer would hang on a bare @f — we use a deref timeout.)"
  [& {:keys [n] :or {n 5}}]
  (println "\n========== SCENARIO A — naive: timeout=>transient=>retry-storm ==========")
  (reset-clock!)
  (let [backend (make-backend {:service-ms 300})
        sup (c/create-supervisor
             {:name   "naive"
              :log-fn timeline-log-fn
              :policy (policy/make-reference-policy {})})]
    (try
      (let [futs    (submit-batch sup backend n {:max-attempts 2 :client-timeout-ms 500})
            results (await-all futs 6000)]
        (println "--- parked-tasks after the storm (explain-stuck): ---")
        (explain-stuck sup)
        (report "A/naive" backend results)
        results)
      (finally (c/stop! sup) (shutdown-backend! backend)))))

(defn scenario-throttle
  "B) POLICY LEVER (a TRAP — kept because the failure is the lesson): reclassify a
   timeout as :rate-limited (like a 429). The instinct is that the supervisor-wide
   throttle is the 'slow backend => back everything off' primitive. It BACKFIRES
   into a livelock. FINDINGS: (1) throttle is TIME pacing with NO concurrency
   bound — when the window lifts, ALL siblings release AT ONCE onto the one worker
   and re-pile (thundering herd). (2) a 429 deliberately does NOT consume the retry
   budget (being throttled isn't evidence the task is broken), so tasks NEVER park
   or exhaust — they retry UNBOUNDEDLY. Net: worse than naive (far more wasted jobs,
   nothing parks, ~nothing completes). This is the sharpest evidence that the real
   need is a CONCURRENCY bound (C), not time pacing."
  [& {:keys [n] :or {n 5}}]
  (println "\n========== SCENARIO B — policy lever: timeout treated as throttle ==========")
  (reset-clock!)
  (let [backend (make-backend {:service-ms 300})
        timeout-as-rate (fn [e] (if (= :timeout (:type (ex-data e))) :rate-limited :transient))
        sup (c/create-supervisor
             {:name   "throttle"
              :log-fn timeline-log-fn
              :policy (policy/make-reference-policy
                        {:classify-error timeout-as-rate :default-throttle-ms 350})})]
    (try
      (let [futs    (submit-batch sup backend n {:max-attempts 2 :client-timeout-ms 500})
            results (await-all futs 6000)]
        (report "B/throttle" backend results)
        results)
      (finally (c/stop! sup) (shutdown-backend! backend)))))

(defn scenario-single-flight
  "C) CONCURRENCY-LIMIT PRIMITIVE (capture, don't decide — §12.3): bound in-flight
   submissions to the worker count with a client-side semaphore. A request only
   reaches the worker when the worker is free, so it NEVER times out => 0 retries,
   0 parks, minimal wall time, peak backlog = permits. This is a DISTINCT axis
   from retry/backoff and cleanly removes the pain the other levers only soften."
  [& {:keys [n permits] :or {n 5 permits 1}}]
  (println (format "\n========== SCENARIO C — concurrency limit (single-flight, permits=%d) ==========" permits))
  (reset-clock!)
  (let [backend (make-backend {:service-ms 300})
        sem (Semaphore. permits)
        sup (c/create-supervisor
             {:name "single-flight" :log-fn timeline-log-fn
              :policy (policy/make-reference-policy {})})]
    (try
      (let [promises
            (mapv (fn [i]
                    (let [p (promise)]
                      (Thread/startVirtualThread
                       (fn []
                         (.acquire sem)
                         (emit! "gate" (str "req-" i " admitted (permits left "
                                            (.availablePermits sem) ")"))
                         (let [prompt (str "req-" i)
                               f (c/submit sup {:prompt prompt} (call backend prompt {:client-timeout-ms 500}))]
                           (deliver p (try (deref f 20000 :TIMED-OUT)
                                           (catch Throwable t (str "ABORTED: " (ex-message t)))))
                           (.release sem))))
                      p))
                  (range n))
            results (mapv deref promises)]
        (report "C/single-flight" backend results)
        results)
      (finally (c/stop! sup) (shutdown-backend! backend)))))

(defn scenario-co-supervisor
  "D) CO-SUPERVISOR / UNATTENDED (§12.2): keep a policy that PARKS on the first
   timeout (max-attempts 1 — no retry storm), then let a hand-written reference
   co-supervisor service every park by WAITING for the worker to drain and
   retrying once. The unattended run completes correctly with NO library-level
   :on-exhaustion :abort — evidence the co-supervisor collapses the worst case
   (bears on Q4). Reconciliation, not the lossy tap, is what guarantees no park
   is missed."
  [& {:keys [n] :or {n 5}}]
  (println "\n========== SCENARIO D — reference co-supervisor services parks (unattended) ==========")
  (reset-clock!)
  (let [backend (make-backend {:service-ms 300})
        sup (c/create-supervisor
             {:name "co-sup" :log-fn timeline-log-fn
              :policy (policy/make-reference-policy {})})
        stop-cosup (start-co-supervisor sup backend {:max-retries 3})]
    (try
      (let [futs    (submit-batch sup backend n {:max-attempts 1 :client-timeout-ms 500})
            results (await-all futs 6000)]
        (report "D/co-sup" backend results)
        results)
      (finally (stop-cosup) (c/stop! sup) (shutdown-backend! backend)))))

;; =============================================================================
;; 8. Experiments E1–E3 (RI 13): pin down the SHAPE of the concurrency bound
;; =============================================================================
;; Scenario C proved the bound is real, but only for a single-worker backend with
;; one in-process gate. These probe the three questions that gate the DESIGN
;; decision (consumer recipe vs. library surface): does it generalize past W=1
;; (E1); WHERE must the gate live to actually bound the backend (E2); and does it
;; COMPOSE with the co-supervisor (E3)?

(defn scenario-multi-worker
  "E1) Does the bound generalize past a single worker? Backend has W workers; we
   admit `permits` in-flight. Claim: THE KNOB IS WORKER COUNT. permits=W => full
   utilization, 0 waste, peak backlog=W. permits>W => over-admission re-introduces
   pile-up (timeouts/retries return). permits<W => underutilized (still 0 waste,
   just slower). Run it three ways to see the knob."
  [& {:keys [n workers permits] :or {n 6 workers 2 permits 2}}]
  (println (format "\n========== E1 — multi-worker: workers=%d permits=%d n=%d ==========" workers permits n))
  (reset-clock!)
  (let [backend (make-backend {:service-ms 300 :workers workers})
        sem (Semaphore. permits)
        sup (c/create-supervisor {:name "multi-worker" :log-fn timeline-log-fn
                                  :policy (policy/make-reference-policy {})})]
    (try
      (let [results (gated-run sup backend sem n {:client-timeout-ms 500})]
        (report (format "E1/w%d-p%d" workers permits) backend results)
        results)
      (finally (c/stop! sup) (shutdown-backend! backend)))))

(defn scenario-gate-scope
  "E2) WHERE must the bound live? ONE shared worker (W=1). Two independent consumers
   each submit half the requests.
     :local  => each consumer holds its OWN Semaphore(1). Each serializes ITSELF,
               but aggregate in-flight = 2 on one worker => the worker saturates and
               pile-up RETURNS. A per-call-site bound does NOT compose.
     :shared => both consumers share ONE Semaphore(1) => aggregate in-flight bounded
               => clean, 0 waste.
   Lesson: the bound is a property of the BACKEND, not the call site. To bound a
   backend shared by N consumers you need ONE gate they all pass through — the
   argument for a library/supervisor-level gate over ad-hoc consumer semaphores."
  [& {:keys [n mode] :or {n 6 mode :local}}]
  (println (format "\n========== E2 — gate scope: %s (one worker, two consumers, n=%d) ==========" (name mode) n))
  (reset-clock!)
  (let [backend (make-backend {:service-ms 300 :workers 1})
        half    (quot n 2)
        shared  (Semaphore. 1)
        sem-a   (if (= mode :shared) shared (Semaphore. 1))
        sem-b   (if (= mode :shared) shared (Semaphore. 1))
        sup (c/create-supervisor {:name "gate-scope" :log-fn timeline-log-fn
                                  :policy (policy/make-reference-policy {})})]
    (try
      (let [pa (future (gated-run sup backend sem-a half
                                  {:client-timeout-ms 500 :first-req 0 :label "consumerA"}))
            pb (future (gated-run sup backend sem-b (- n half)
                                  {:client-timeout-ms 500 :first-req half :label "consumerB"}))
            results (into (deref pa) (deref pb))]
        (report (format "E2/%s" (name mode)) backend results)
        results)
      (finally (c/stop! sup) (shutdown-backend! backend)))))

(defn scenario-bound-plus-cosup
  "E3) Do PREVENTION (concurrency bound) and RECOVERY (co-supervisor) COMPOSE? A
   shared gate (permits=W) bounds saturation so nothing times out from pile-up —
   but a `fail` subset hits a flaky-endpoint ERROR the bound cannot prevent (not
   saturation); with max-attempts 1 they PARK. A co-supervisor services those parks.
   Claim: complementary, non-conflicting — the gate makes parks RARE (only genuine
   failures), and the co-sup mops those up. Peak backlog stays = permits throughout;
   the co-sup drives the parked requests to completion so every permit is released."
  [& {:keys [n workers permits fail] :or {n 6 workers 1 permits 1 fail #{1 3}}}]
  (println (format "\n========== E3 — bound + co-supervisor: workers=%d permits=%d fail=%s ==========" workers permits fail))
  (reset-clock!)
  (let [backend (make-backend {:service-ms 300 :workers workers})
        _       (reset! (:fail-once backend) (set (map #(str "req-" %) fail)))
        sem (Semaphore. permits)
        sup (c/create-supervisor {:name "bound+cosup" :log-fn timeline-log-fn
                                  :policy (policy/make-reference-policy {})})
        stop-cosup (start-co-supervisor sup backend {:max-retries 3})]
    (try
      (let [results (gated-run sup backend sem n {:client-timeout-ms 500 :max-attempts 1})]
        (report (format "E3/w%d-p%d" workers permits) backend results)
        results)
      (finally (stop-cosup) (c/stop! sup) (shutdown-backend! backend)))))

;; =============================================================================
;; 9. Experiments E4–E5 (RI 14): the two interactions that gate LIBRARY surface
;; =============================================================================
;; E4 asks whether a concurrency bound FIGHTS the supervisor-wide throttle or
;; COMPOSES with it. E5 prototypes a SUBMIT-SIDE gate (library-style, permit
;; lifecycle owned by the gate not the consumer) to feel out the release contract:
;; when should a permit be handed back — on terminal state, or as soon as a task
;; parks? Both are the open questions from RI 13 §5.

(defn scenario-throttle-bound
  "E4) Does a concurrency bound FIGHT the supervisor-wide throttle, or COMPOSE with
   it? Backend is slow (1 worker) AND returns a REAL 429 (retry-after 300ms) on a
   subset of prompts. The 429 throttles the whole supervisor (§3); the gate bounds
   in-flight. :gated? false => no gate (throttle alone — the concurrency bound that
   scenario B was missing); :gated? true => gate permits=1. Claim: they compose on
   ORTHOGONAL axes (time-pacing vs concurrency), so the gate removes the post-lift
   thundering herd that made throttle-alone livelock in B."
  [& {:keys [n gated? rate-limit] :or {n 5 gated? true rate-limit #{2}}}]
  (println (format "\n========== E4 — bound x throttle: gated?=%s rate-limit=%s ==========" gated? rate-limit))
  (reset-clock!)
  (let [backend (make-backend {:service-ms 300 :workers 1})
        _ (reset! (:rate-limit-once backend) (set (map #(str "req-" %) rate-limit)))
        sem (Semaphore. 1)
        sup (c/create-supervisor {:name "throttle-bound" :log-fn timeline-log-fn
                                  :policy (policy/make-reference-policy {})})]
    (try
      (let [results (if gated?
                      (gated-run sup backend sem n {:client-timeout-ms 500 :max-attempts 3})
                      (await-all (submit-batch sup backend n {:max-attempts 3 :client-timeout-ms 500}) 8000))]
        (report (format "E4/%s" (if gated? "gated" "nogate")) backend results)
        results)
      (finally (c/stop! sup) (shutdown-backend! backend)))))

(defn- submit-gated
  "SUBMIT-SIDE gate PROTOTYPE (library-style): the permit lifecycle is owned here,
   not by the consumer. Acquire a permit (bounded wait so a wedged gate is visible
   instead of an infinite hang), submit, then a watcher releases the permit per the
   RELEASE CONTRACT:
     :terminal => release only when the task reaches :resolved/:aborted/:cancelled —
                  holds the permit across retries AND parks. Simple and keeps the
                  bound exact, but a park with no co-sup/deref-timeout WEDGES the
                  permit (the E5 footgun).
     :on-park  => release as soon as the task parks — frees capacity so parked work
                  can't wedge the gate, but parked tasks then sit OUTSIDE the bound
                  (a co-sup retrying them re-enters unbounded in this prototype).
   Returns the future, or :gate-timeout if no permit came free in time."
  [sup backend sem prompt {:keys [release client-timeout-ms max-attempts admit-timeout-ms]
                           :or {release :terminal client-timeout-ms 500 max-attempts 1
                                admit-timeout-ms 2500}}]
  (if-not (.tryAcquire ^Semaphore sem (long admit-timeout-ms) TimeUnit/MILLISECONDS)
    (do (emit! "gate" (str prompt " NOT admitted (gate wedged — permit never freed)"))
        :gate-timeout)
    (do
      (emit! "gate" (str prompt " admitted (permits left " (.availablePermits ^Semaphore sem)
                         ", release=" (name release) ")"))
      (let [f (c/submit sup {:prompt prompt :dj.concurrency/max-attempts max-attempts}
                        (call backend prompt {:client-timeout-ms client-timeout-ms}))
            released (atom false)
            release! (fn [why] (when (compare-and-set! released false true)
                                 (emit! "gate" (str prompt " permit released (" why ")"))
                                 (.release ^Semaphore sem)))]
        (Thread/startVirtualThread
         (fn []
           (loop []
             (let [st (:status (c/task f))]
               (cond
                 (#{:resolved :aborted :cancelled} st) (release! (name st))
                 (and (= release :on-park) (= st :parked)) (release! "on-park")
                 :else (do (Thread/sleep 15) (recur)))))))
        f))))

(defn scenario-submit-side-gate
  "E5) SUBMIT-SIDE gate + release contract. One worker, permits=1, a `fail` subset
   parks (max-attempts 1). Combinations worth running (see run-experiments-2):
     :terminal + co-sup  => clean baseline (co-sup drives parks terminal -> released).
     :terminal + NO co-sup => the parked task WEDGES its permit; with permits=1 the
                              whole gate stalls and later requests are never admitted
                              (:gate-timeout). The footgun the recipe warns about.
     :on-park  + NO co-sup => no wedge (others complete), but the parked task stays
                              parked (nobody recovers it) -> it TIMED-OUT on its own.
     :on-park  + co-sup   => no wedge AND recovered."
  [& {:keys [n permits release co-sup? fail] :or {n 5 permits 1 release :terminal co-sup? true fail #{1}}}]
  (println (format "\n========== E5 — submit-side gate: release=%s co-sup=%s permits=%d fail=%s =========="
                   (name release) co-sup? permits fail))
  (reset-clock!)
  (let [backend (make-backend {:service-ms 300 :workers 1})
        _ (reset! (:fail-once backend) (set (map #(str "req-" %) fail)))
        sem (Semaphore. permits)
        sup (c/create-supervisor {:name "submit-gate" :log-fn timeline-log-fn
                                  :policy (policy/make-reference-policy {})})
        stop-cosup (when co-sup? (start-co-supervisor sup backend {:max-retries 3}))]
    (try
      (let [ps (mapv (fn [i]
                       (let [p (promise)]
                         (Thread/startVirtualThread
                          (fn []
                            (let [f (submit-gated sup backend sem (str "req-" i)
                                                  {:release release :max-attempts 1})]
                              (deliver p (if (= :gate-timeout f)
                                           ":WEDGED"
                                           (try (deref f 4000 :TIMED-OUT)
                                                (catch Throwable t (str "ABORTED: " (ex-message t)))))))))
                         p))
                     (range n))
            results (mapv deref ps)]
        (report (format "E5/%s-cosup%s" (name release) co-sup?) backend results)
        (println "  outcomes detail:" (pr-str (frequencies results)))
        results)
      (finally (when stop-cosup (stop-cosup)) (c/stop! sup) (shutdown-backend! backend)))))

(defn run-all
  "Runs A -> B -> C -> D as one narrative. Read the timelines top-to-bottom; the
   REPORT lines quantify wasted worker jobs and peak backlog per lever."
  [& _]
  (scenario-naive)
  (scenario-throttle)
  (scenario-single-flight)
  (scenario-co-supervisor)
  (println "\n========== done — compare REPORT lines: wasted jobs & peak backlog per lever ==========")
  :done)

(defn run-experiments
  "RI 13: runs E1 (three ways) -> E2 (both scopes) -> E3. Read the REPORT lines:
   E1 shows permits=W is the sweet spot; E2 shows only the SHARED gate stays clean;
   E3 shows the gate + co-supervisor compose (bound prevents saturation, co-sup
   recovers the genuine failures)."
  [& _]
  (scenario-multi-worker :workers 2 :permits 2)   ; permits=W: clean
  (scenario-multi-worker :workers 2 :permits 4)   ; permits>W: over-admission
  (scenario-multi-worker :workers 2 :permits 1)   ; permits<W: underutilized
  (scenario-gate-scope :mode :local)              ; per-call-site: saturates
  (scenario-gate-scope :mode :shared)             ; shared: clean
  (scenario-bound-plus-cosup)                     ; prevention + recovery
  (println "\n========== experiments done — compare REPORT lines ==========")
  :done)

(defn run-experiments-2
  "RI 14: the two interactions that gate LIBRARY surface.
   E4 (bound x throttle): no-gate control vs gated — the gate supplies the
   concurrency bound the throttle lacks, so it composes instead of fighting.
   E5 (submit-side gate release contract): :terminal is simple but a park with no
   co-sup WEDGES the permit; :on-park avoids the wedge but puts parked work outside
   the bound. Read the REPORT + outcomes-detail lines."
  [& _]
  (scenario-throttle-bound :gated? false)                       ; E4 control
  (scenario-throttle-bound :gated? true)                        ; E4 gated: composes
  (scenario-submit-side-gate :release :terminal :co-sup? true)  ; baseline
  (scenario-submit-side-gate :release :terminal :co-sup? false) ; WEDGE footgun
  (scenario-submit-side-gate :release :on-park  :co-sup? false) ; no wedge, unrecovered
  (scenario-submit-side-gate :release :on-park  :co-sup? true)  ; no wedge, recovered
  (println "\n========== experiments-2 done — compare REPORT + outcomes-detail ==========")
  :done)

(comment
  (require 'dj.concurrency.playground :reload)
  (dj.concurrency.playground/run-all)
  ;; or drive individual levers:
  (scenario-naive :n 6)
  (scenario-single-flight :n 6 :permits 1)

  ;; RI 13 experiments (shape of the concurrency bound):
  (dj.concurrency.playground/run-experiments)
  (scenario-multi-worker :workers 3 :permits 3)
  (scenario-gate-scope :mode :local)
  (scenario-gate-scope :mode :shared)
  (scenario-bound-plus-cosup :fail #{1 3})

  ;; RI 14 experiments (bound x throttle + submit-side gate release contract):
  (dj.concurrency.playground/run-experiments-2)
  (scenario-throttle-bound :gated? true)
  (scenario-submit-side-gate :release :terminal :co-sup? false)
  (scenario-submit-side-gate :release :on-park :co-sup? true)
  )
