(ns dj.concurrency.policy
  "Functional core for dj.concurrency: the pure reference policy that decides
   what should happen, with no side effects. The impure execution of those
   decisions lives in `dj.concurrency.shell`.

   A policy is a pure function `(fn [event state] -> {:directives [...] :state s'})`.
   Events and directives are positional pairs `[type payload-map]`; every other
   value is a MAP so the code reads by key rather than by position.

   NOTE: the qualified keywords `:dj.concurrency/attempts`,
   `:dj.concurrency/max-attempts`, and `:dj.concurrency/shutdown` are part of the
   public contract — consumers set `:dj.concurrency/max-attempts` in a task's
   context and match on `:dj.concurrency/shutdown` in ex-data. They are keyed to
   the `dj.concurrency` namespace, so they are written out in full here rather
   than with `::` auto-resolution (which would key them to THIS namespace).")

;; The three statuses a task can never leave: safe to prune, safe to drop the CF.
(def terminal-statuses #{:resolved :aborted :cancelled})

;; State sentinel: a pool that is REGISTERED but carries no concurrency cap.
;; Distinct from "absent" so a first-seen (undeclared) pool can be told apart
;; from a declared-unbounded one — that distinction is what makes the
;; :unknown-pool warn fire exactly once. Keyed to the dj.concurrency namespace.
(def ^:private pool-unbounded :dj.concurrency/unbounded)

(defn- task-pool
  "The resource pool a task is bound to: an opaque `:dj.concurrency/pool` context
   tag (1:1 with an external resource, or an API-key/tenant/rate-limit bucket —
   the policy never interprets it). Untagged tasks fall into :default."
  [t]
  (get-in t [:context :dj.concurrency/pool] :default))

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
   (or by passing them to `create-supervisor`).

   Per-pool concurrency caps are deliberately NOT here: they are runtime-updatable
   and so live in supervisor STATE (`:pool-caps {pool cap}`, seeded by
   `create-supervisor` from its `:pool-caps` opt and written live by
   `set-pool-cap!`), not in this frozen config closure. A cap is either a positive
   integer or the `:dj.concurrency/unbounded` sentinel; in-flight is bounded PER
   POOL at the single admission chokepoint (`scan-deadlines`), a permit == a
   :running slot. No `:pool-caps` ⇒ one unbounded :default pool ⇒ today's exact
   behavior. See the multi-resource design doc."
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
                               :error (ex-info "supervisor stopped" {:type :dj.concurrency/shutdown})}]]
         :state state}

        ;; S2: Always enqueue. scan-deadlines is the SINGLE per-pool admission
        ;; chokepoint; it admits this task the same pass when the task's pool has a
        ;; free permit (an unbounded pool always does). Collapsing the old
        ;; bounded/unbounded + throttled submit forks into one queue path is what
        ;; funnels every worker-bound route through one gate — per pool (F11).
        ;; ::attempts is seeded to 1 so a later failure never increments a missing
        ;; counter.
        (let [ctx'   (assoc (:context payload) :dj.concurrency/attempts 1)
              pool   (get ctx' :dj.concurrency/pool :default)
              known? (contains? (:pool-caps state) pool)]
          {:directives (cond-> [[:log {:level :debug :event :submit-queued :task-id task-id
                                       :data {:task-id task-id :pool pool}}]]
                         ;; U-lifecycle "middle": a submit to a pool with no declared
                         ;; cap runs UNBOUNDED but warns ONCE — a typo'd tag is loud
                         ;; instead of silently escaping its intended bound.
                         (not known?)
                         (conj [:log {:level :warn :event :unknown-pool :task-id task-id
                                      :data {:task-id task-id :pool pool}}]))
           :state (cond-> (assoc-in state [:tasks task-id]
                                    {:task-id task-id, :status :queued
                                     :context ctx', :closure (:closure payload)
                                     :submitted-at (:submitted-at payload)})
                    ;; Register the pool so the warn never re-fires and `state`
                    ;; lists every pool ever touched (one authoritative registry).
                    (not known?) (assoc-in [:pool-caps pool] pool-unbounded))}))

      ;; --- K: Success ---
      :success
      (cond
        ;; K1: Late/Ignored
        (or (nil? t) (terminal-statuses status))
        {:directives [[:log {:level :warn :event :late-success :task-id task-id :data task-id}]] :state state}

        ;; K2: Expected Success
        (= :running status)
        {:directives [[:resolve {:task-id task-id :result (:result payload)}]]
         ;; Enrichment only: carry the store's :cached? provenance onto the task
         ;; so `(task f)` reports whether a result came from the durable memo.
         ;; Policies that ignore :cached? lose nothing but the annotation.
         :state (update-in state [:tasks task-id]
                           (fn [t] (cond-> (assoc t :status :resolved)
                                     (:cached? payload) (assoc :cached? true))))}

        ;; K3: Illegal
        :else
        {:directives [[:log {:level :warn :event :illegal-transition :task-id task-id :data task-id}]] :state state})

      ;; --- F: Failed ---
      :failed
      (cond
        ;; F1: Late/Ignored
        (or (nil? t) (terminal-statuses status))
        {:directives [[:log {:level :warn :event :late-failure :task-id task-id :data task-id}]] :state state}

        (= :running status)
        (let [pool         (task-pool t)
              err-type     ((:classify-error config) (:error payload))
              attempts     (get-in t [:context :dj.concurrency/attempts] 1)
              max-attempts (get-in t [:context :dj.concurrency/max-attempts] (:max-attempts config))]
          (case err-type
            ;; F2: Fatal
            :fatal
            {:directives [[:abort {:task-id task-id :error (:error payload)}]]
             :state (update-in state [:tasks task-id] assoc :status :aborted :error (:error payload))}

            ;; F3: Rate Limited (429)
            :rate-limited
            (let [window  (or (:retry-after (ex-data (:error payload))) (:default-throttle-ms config))
                  wake-at (+ now window)]
              ;; Emit a first-class :throttle-wait event carrying :pool: a 429
              ;; pauses only THIS pool (per-pool throttle), so a saturated backend
              ;; can't false-couple the others. Previously this branch was SILENT,
              ;; which hid the throttle from the tap (playground finding F1).
              {:directives [[:log {:level :info :event :throttle-wait :task-id task-id
                                   :data {:task-id task-id :pool pool :wake-in-ms window}}]]
               ;; PER-POOL window (replaces the global scalar): a concurrent,
               ;; shorter 429 must not shorten a live window, so keep the later
               ;; expiry. A 429 doesn't consume the retry budget (being throttled
               ;; isn't evidence the task is broken). :throttle? tags the waiter so
               ;; clear-throttle can wake exactly these tasks.
               :state (-> state
                          (update-in [:pool-throttle pool] (fnil max 0) wake-at)
                          (update-in [:tasks task-id] assoc
                                     :status :waiting-retry :wake-at wake-at
                                     :throttle? true :error (:error payload)))})

            ;; F4 & F5: Transient
            :transient
            (if (< attempts max-attempts)
              ;; F4: Retryable — schedule a backed-off retry. Emit a first-class
              ;; :retry-scheduled event (routine per-task step, :debug) so a
              ;; retry-storm is visible from the tap. Previously SILENT, which is
              ;; exactly what hid pile-up on a saturated single-worker backend
              ;; (playground finding F1). :attempt is the NEXT attempt number.
              (let [backoff ((:backoff-fn config) attempts)]
                {:directives [[:log {:level :debug :event :retry-scheduled :task-id task-id
                                     :data {:task-id      task-id
                                            :attempt      (inc attempts)
                                            :max-attempts max-attempts
                                            :wake-in-ms   backoff}}]]
                 :state (-> state
                            (update-in [:tasks task-id] assoc
                                       :status :waiting-retry :wake-at (+ now backoff) :error (:error payload))
                            (update-in [:tasks task-id :context :dj.concurrency/attempts] (fnil inc 1)))})

              ;; F5: Exhausted / Parked
              {:directives [[:log {:level :info :event :parked :task-id task-id :data task-id}]]
               :state (update-in state [:tasks task-id] assoc :status :parked :error (:error payload))})))

        ;; F6: Illegal
        :else
        {:directives [[:log {:level :warn :event :illegal-transition :task-id task-id :data task-id}]] :state state})

      ;; --- T: Tick ---
      :tick
      {:directives [] :state state} ;; Handled purely by the deadline-scan post-step

      ;; --- R: REPL Retry ---
      :repl/retry
      (cond
        ;; R1 — a REPL retry grants a fresh attempt budget ("I changed something").
        ;; Route to :queued so the retry re-acquires through its POOL's gate at
        ;; scan-deadlines (F11 per pool: the pool tag rides on :context, so a
        ;; co-sup/REPL retry never re-enters uncounted or in the wrong pool — the
        ;; F10 :on-park leak). One chokepoint means no bounded/unbounded fork here.
        (#{:parked :waiting-retry :queued} status)
        {:directives [[:log {:level :debug :event :retry-queued :task-id task-id
                             :data {:task-id task-id :pool (task-pool t)}}]]
         :state (update-in state [:tasks task-id] assoc
                           :status :queued :wake-at nil :throttle? nil
                           :context (assoc-in (:context t) [:dj.concurrency/attempts] 1))}
        ;; R2 & R3
        (= :running status)
        {:directives [[:log {:level :warn :event :already-running :task-id task-id :data task-id}]] :state state}
        :else
        {:directives [[:log {:level :warn :event :no-such-task :task-id task-id :data task-id}]] :state state})

      ;; --- D: REPL Deliver ---
      ;; Deliver/abort/cancel all clear :wake-at: a terminal task must never leave
      ;; a live deadline behind, or the deadline scan spins on it.
      :repl/deliver
      (if (#{:parked :waiting-retry :queued :running} status)
        {:directives [[:resolve {:task-id task-id :result (:result payload)}]]
         :state (update-in state [:tasks task-id] assoc :status :resolved :wake-at nil)}
        {:directives [[:log {:level :warn :event :invalid-deliver :task-id task-id :data task-id}]] :state state})

      ;; --- A: REPL Abort ---
      :repl/abort
      (if (#{:parked :waiting-retry :queued :running} status)
        {:directives [[:abort {:task-id task-id :error (:error payload)}]]
         :state (update-in state [:tasks task-id] assoc :status :aborted :wake-at nil)}
        {:directives [[:log {:level :warn :event :invalid-abort :task-id task-id :data task-id}]] :state state})

      ;; --- C: REPL Cancel ---
      :repl/cancel
      (cond
        ;; C1 — :drop-cf stops the shell tracking the never-completed CF;
        ;; consumer semantics are unchanged (see `cancel`).
        (#{:parked :waiting-retry :queued} status)
        {:directives [[:drop-cf {:task-id task-id}]
                      [:log {:level :info :event :cancelled :task-id task-id :data task-id}]]
         :state (update-in state [:tasks task-id] assoc :status :cancelled :wake-at nil)}
        ;; C2 & C3
        (= :running status)
        {:directives [[:log {:level :warn :event :cannot-cancel-running :task-id task-id :data task-id}]] :state state}
        :else
        {:directives [[:log {:level :warn :event :invalid-cancel :task-id task-id :data task-id}]] :state state})

      ;; --- X: REPL Clear Throttle ---
      ;; Lift a pool's throttle window (or ALL pools' when :pool is absent) and
      ;; mark that pool's throttle-tagged waiters due, so the scan-deadlines
      ;; post-step drains both the queued tasks and the 429'd task in this pass.
      :repl/clear-throttle
      (let [pool (:pool payload)]                 ;; nil => clear every pool
        {:directives []
         :state (-> state
                    (update :pool-throttle (fn [pt] (if pool (dissoc pt pool) {})))
                    (update :tasks
                            (fn [ts]
                              (into {}
                                    (map (fn [[id t]]
                                           [id (if (and (= :waiting-retry (:status t))
                                                        (:throttle? t)
                                                        (or (nil? pool) (= pool (task-pool t))))
                                                 (assoc t :wake-at now)
                                                 t)]))
                                    ts))))})

      ;; --- SC: REPL Set Pool Cap ---
      ;; Runtime-updatable per-pool concurrency bound. Writes the cap into STATE
      ;; (not a frozen config closure), so an operator can retune a live pool —
      ;; and a future signal-learned bound (U8) would write the same slot. Lowering
      ;; below current in-flight is safe: scan just admits 0 until the pool drains
      ;; below the new cap; no running task is touched.
      :repl/set-pool-cap
      {:directives [[:log {:level :info :event :pool-cap-set
                           :data {:pool (:pool payload) :cap (:cap payload)}}]]
       :state (assoc-in state [:pool-caps (:pool payload)] (:cap payload))}

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
                                             :error   (ex-info "supervisor stopped" {:type :dj.concurrency/shutdown})}])
                                  non-terminals)
              state'        (reduce (fn [s task] (assoc-in s [:tasks (:task-id task) :status] :aborted))
                                    state non-terminals)]
          {:directives dirs :state (assoc state' :shutdown? true)})

        :drop
        {:directives [] :state (assoc state :shutdown? true)})

      ;; --- W1: Wildcard ---
      {:directives [[:log {:level :error :event :unknown-event :data event-type}]] :state state})))

(defn- scan-deadlines
  "Runs after every event (including ticks): the PER-POOL admission chokepoint +
   deadline drain. Takes and returns a {:directives [...] :state {...}} map.
   Corresponds to rows T-a, T-b, and T-c in the spec.

   For EACH pool independently: expire its throttle window if due; count its
   in-flight (:running) tasks; drain its eligible set (queued + due-retry)
   oldest-first (FIFO, U4) up to the free-permit budget (W_r − in-flight). A pool
   whose cap is unbounded (or as-yet unregistered) drains fully; a throttled pool
   admits nothing. No cross-pool coordination — a saturated or 429'd pool never
   holds up another.

   THE CHOKEPOINT (RI 16 / A2, generalized per pool): every worker-bound path
   (submit, timed retry, repl/co-sup retry) routes to :queued, so bounding
   admissions here bounds ALL of them from one point — F11, per pool. A permit ==
   a :running slot; in-flight is just the per-pool count of :running tasks, so a
   park/backoff/terminal transition frees a permit with no bookkeeping. Admission
   observability (:admission-granted / :admission-wait, both carrying :pool) fires
   only for CAPPED pools, so an unbounded pool drains as silently as today's
   unbounded supervisor."
  [{:keys [directives state]} _config now]
  (let [pool-caps  (:pool-caps state)
        ;; 1. Expire per-pool throttle windows whose deadline has passed.
        pt'        (into {} (remove (fn [[_ at]] (<= at now)) (:pool-throttle state)))
        state-1    (assoc state :pool-throttle pt')
        throttled? (fn [pool] (contains? pt' pool))
        all        (vals (:tasks state-1))

        ;; 2. In-flight per pool = count of :running tasks by pool tag.
        running-by-pool (frequencies (map task-pool (filter #(= :running (:status %)) all)))

        ;; 3. Eligible (T-a queued / REPL clears + T-b due retries), EXCLUDING any
        ;; task whose pool is currently throttled.
        eligible   (filter (fn [t]
                             (and (not (throttled? (task-pool t)))
                                  (or (= :queued (:status t))
                                      (and (= :waiting-retry (:status t))
                                           (:wake-at t) (<= (:wake-at t) now)))))
                           all)

        ;; 4. Group by pool; drain each independently, oldest-first up to its
        ;; free-permit budget. Unbounded/unregistered pool ⇒ full drain.
        by-pool    (into {} (map (fn [[p ts]] [p (sort-by :submitted-at ts)]))
                         (group-by task-pool eligible))
        drain      (reduce
                    (fn [acc [pool ts]]
                      (let [cap    (get pool-caps pool)
                            capped (number? cap)
                            inflt  (get running-by-pool pool 0)
                            budget (if capped (max 0 (- cap inflt)) (count ts))]
                        (-> acc
                            (update :admit into (take budget ts))
                            (update :block into (drop budget ts))
                            (assoc-in [:capped pool] capped)
                            (assoc-in [:inflt pool] inflt)
                            (assoc-in [:cap pool] cap))))
                    {:admit [] :block [] :capped {} :inflt {} :cap {}}
                    by-pool)
        admit      (:admit drain)
        blocked    (:block drain)

        ;; 5. Execute directives (+ admission observability, capped pools only).
        exec-dirs  (map (fn [t] [:execute (select-keys t [:task-id :context :closure])]) admit)
        grant-logs (for [t admit
                         :let  [p (task-pool t)]
                         :when (get-in drain [:capped p])]
                     [:log {:level :debug :event :admission-granted :task-id (:task-id t)
                            :data {:task-id (:task-id t) :pool p
                                   :in-flight (get-in drain [:inflt p]) :cap (get-in drain [:cap p])}}])
        ;; :admission-wait fires ONCE per task (only when newly blocked), the twin
        ;; of :throttle-wait — so a saturated pool is legible without per-event spam.
        wait-logs  (for [t blocked
                         :let  [p (task-pool t)]
                         :when (and (get-in drain [:capped p]) (not (:admission-waiting? t)))]
                     [:log {:level :info :event :admission-wait :task-id (:task-id t)
                            :data {:task-id (:task-id t) :pool p
                                   :in-flight (get-in drain [:inflt p]) :cap (get-in drain [:cap p])}}])

        ;; 6. Advance admitted tasks to :running (clear :throttle?/:admission-waiting?);
        ;; tag newly-blocked tasks so the wait event doesn't re-fire next pass.
        state-2    (as-> state-1 s
                     (reduce (fn [s t]
                               (update-in s [:tasks (:task-id t)]
                                          assoc :status :running :wake-at nil
                                          :throttle? nil :admission-waiting? nil))
                             s admit)
                     (reduce (fn [s t]
                               (assoc-in s [:tasks (:task-id t) :admission-waiting?] true))
                             s blocked))]

    ;; T-c (expiry/TTL logic) is reserved for v2.
    {:directives (into [] (concat directives grant-logs wait-logs exec-dirs))
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
   Per-pool concurrency caps are NOT a policy opt — they live in supervisor state
   (`:pool-caps`, seeded by `create-supervisor`, updatable via `set-pool-cap!`).
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
            (scan-deadlines config now))))))
;; NOTE: `->` threads the apply-event result map in as scan-deadlines' FIRST arg,
;; so the signature is [result-map config now].

(def default-policy
  "The reference policy built with all defaults; equals `(make-reference-policy {})`."
  (make-reference-policy {}))
