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
   :default-throttle-ms 5000
   ;; Concurrency admission (RI 16 / approach A2). nil = unbounded (today's exact
   ;; behavior; every path below is inert). When set to W, the policy bounds
   ;; concurrent :running tasks to W: a permit == a :running slot, so a task that
   ;; leaves :running (park, backoff, terminal) frees it, and every worker-bound
   ;; path (submit, timed retry, repl/co-sup retry) re-acquires through the SINGLE
   ;; chokepoint below (scan-deadlines). This is the concurrency-axis twin of the
   ;; time-axis throttle. See ledger/concurrency_limit_approaches_2026_07_06.md.
   :max-in-flight       nil})

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

        (let [throttled? (and (:throttle-expires-at state)
                              (< now (:throttle-expires-at state)))]
          (if throttled?
            ;; S2: Throttled
            {:directives [[:log {:level :info :event :submit-throttled :data task-id}]]
             :state (assoc-in state [:tasks task-id]
                              {:task-id task-id, :status :queued
                               ;; Seed ::attempts like S3 so a later failure never
                               ;; increments a missing counter.
                               :context (assoc (:context payload) :dj.concurrency/attempts 1)
                               :closure (:closure payload)
                               :submitted-at (:submitted-at payload)})}
            ;; S3: Normal Submit
            (let [ctx' (assoc (:context payload) :dj.concurrency/attempts 1)]
              (if (:max-in-flight config)
                ;; S3b: Bounded — enter the admission queue (like the throttled S2
                ;; path, but on the concurrency axis). scan-deadlines admits it
                ;; the moment a permit is free (often the same pass).
                {:directives [[:log {:level :debug :event :submit-queued :data task-id}]]
                 :state (assoc-in state [:tasks task-id]
                                  {:task-id task-id, :status :queued
                                   :context ctx', :closure (:closure payload)
                                   :submitted-at (:submitted-at payload)})}
                ;; S3a: Unbounded — unchanged direct dispatch.
                {:directives [[:execute {:task-id task-id :context ctx' :closure (:closure payload)}]
                              [:log {:level :debug :event :submit-executed :data task-id}]]
                 :state (assoc-in state [:tasks task-id]
                                  {:task-id task-id, :status :running
                                   :context ctx', :closure (:closure payload)
                                   :submitted-at (:submitted-at payload)})})))))

      ;; --- K: Success ---
      :success
      (cond
        ;; K1: Late/Ignored
        (or (nil? t) (terminal-statuses status))
        {:directives [[:log {:level :warn :event :late-success :data task-id}]] :state state}

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
        {:directives [[:log {:level :warn :event :illegal-transition :data task-id}]] :state state})

      ;; --- F: Failed ---
      :failed
      (cond
        ;; F1: Late/Ignored
        (or (nil? t) (terminal-statuses status))
        {:directives [[:log {:level :warn :event :late-failure :data task-id}]] :state state}

        (= :running status)
        (let [err-type     ((:classify-error config) (:error payload))
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
              ;; Emit a first-class :throttle-wait event: a 429 pauses the WHOLE
              ;; supervisor, so it is supervisor-level notable (:info), not a
              ;; routine per-task step. Previously this branch was SILENT, which
              ;; hid the throttle from the tap (see playground finding F1).
              {:directives [[:log {:level :info :event :throttle-wait
                                   :data {:task-id task-id :wake-in-ms window}}]]
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
              ;; F4: Retryable — schedule a backed-off retry. Emit a first-class
              ;; :retry-scheduled event (routine per-task step, :debug) so a
              ;; retry-storm is visible from the tap. Previously SILENT, which is
              ;; exactly what hid pile-up on a saturated single-worker backend
              ;; (playground finding F1). :attempt is the NEXT attempt number.
              (let [backoff ((:backoff-fn config) attempts)]
                {:directives [[:log {:level :debug :event :retry-scheduled
                                     :data {:task-id      task-id
                                            :attempt      (inc attempts)
                                            :max-attempts max-attempts
                                            :wake-in-ms   backoff}}]]
                 :state (-> state
                            (update-in [:tasks task-id] assoc
                                       :status :waiting-retry :wake-at (+ now backoff) :error (:error payload))
                            (update-in [:tasks task-id :context :dj.concurrency/attempts] (fnil inc 1)))})

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
        (let [t' (assoc-in t [:context :dj.concurrency/attempts] 1)]
          (if (:max-in-flight config)
            ;; R1b: Bounded — a co-sup/REPL retry MUST re-acquire through the same
            ;; admission chokepoint, or it re-enters uncounted and LEAKS the bound
            ;; (RC-2/F11, the F10 :on-park leak). So route to :queued; scan admits.
            {:directives [[:log {:level :debug :event :retry-queued :data task-id}]]
             :state (update-in state [:tasks task-id] assoc
                               :status :queued :wake-at nil :throttle? nil
                               :context (:context t'))}
            ;; R1a: Unbounded — unchanged direct dispatch.
            {:directives [[:execute (select-keys t' [:task-id :context :closure])]]
             :state (update-in state [:tasks task-id] assoc
                               :status :running :wake-at nil :throttle? nil
                               :context (:context t'))}))
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
  "Runs after every event (including ticks). Checks the clock against state
   deadlines and drains tasks accordingly.
   Corresponds to rows T-a, T-b, and T-c in the spec.
   Takes and returns a {:directives [...] :state {...}} map.

   THE ADMISSION CHOKEPOINT (RI 16 / A2): when `config` carries :max-in-flight W,
   this is the single place a task is admitted to a worker. Every worker-bound
   path funnels here (submit and repl-retry route to :queued when bounded; timed
   retries already do), so bounding admissions to the free-permit budget bounds
   ALL of them from one point — satisfying F11 by construction. A permit == a
   :running slot; in-flight is just the count of :running tasks, so a park/backoff/
   terminal transition frees a permit with no bookkeeping."
  [{:keys [directives state]} config now]
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
        eligible (if throttled?
                   []
                   (->> (:tasks state-1)
                        vals
                        (filter (fn [t]
                                  (or (= :queued (:status t))
                                      (and (= :waiting-retry (:status t))
                                           (:wake-at t)
                                           (<= (:wake-at t) now)))))))

        ;; 3b. Concurrency admission (A2): cap the drain to the free-permit budget.
        ;; Unbounded (W nil) → admit everyone (unchanged). Bounded → admit oldest
        ;; first (roughly FIFO) up to (W - in-flight); the rest wait for a permit.
        W          (:max-in-flight config)
        in-flight  (when W (->> (:tasks state-1) vals
                                (filter #(= :running (:status %))) count))
        budget     (when W (max 0 (- W in-flight)))
        eligible*  (if W (sort-by :submitted-at eligible) eligible)
        tasks-to-run (if W (take budget eligible*) eligible*)
        blocked      (if W (drop budget eligible*) [])

        ;; 4. Generate execute directives (+ admission observability under a bound)
        exec-dirs (map (fn [t] [:execute (select-keys t [:task-id :context :closure])])
                       tasks-to-run)
        grant-logs (when W
                     (map-indexed
                      (fn [i t] [:log {:level :debug :event :admission-granted
                                       :data {:task-id       (:task-id t)
                                              :in-flight      (+ in-flight i 1)
                                              :max-in-flight  W}}])
                      tasks-to-run))
        ;; :admission-wait fires ONCE per task (only when newly blocked), the twin
        ;; of :throttle-wait — so a saturated bound is legible without per-event spam.
        wait-logs (when W
                    (for [t blocked
                          :when (not (:admission-waiting? t))]
                      [:log {:level :info :event :admission-wait
                             :data {:task-id       (:task-id t)
                                    :in-flight     in-flight
                                    :max-in-flight W}}]))

        ;; 5. Advance admitted tasks to :running (clear :throttle?/:admission-waiting?);
        ;; tag newly-blocked tasks so the wait event doesn't re-fire next pass.
        state-2  (as-> state-1 s
                   (reduce (fn [s t]
                             (update-in s [:tasks (:task-id t)]
                                        assoc :status :running :wake-at nil
                                        :throttle? nil :admission-waiting? nil))
                           s tasks-to-run)
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
     :max-in-flight       optional concurrency bound W (nil = unbounded, default).
                          When set, at most W tasks are :running at once; excess
                          submits/retries queue and are admitted as permits free.
   See `default-reference-opts`.

   This is how supervisor-level configuration threads into the (otherwise pure,
   2-arg) policy: config is baked in at construction time rather than passed on
   every event. `create-supervisor` calls this with its own opts when you don't
   supply an explicit :policy."
  [opts]
  (let [config (merge default-reference-opts
                      (select-keys opts [:classify-error :backoff-fn
                                         :max-attempts :default-throttle-ms
                                         :max-in-flight]))]
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
