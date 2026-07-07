(ns dj.concurrency.reference-co-supervisor
  "A graduated, TESTED reference co-supervisor for dj.concurrency — a
   copy-paste-quality starting point, NOT part of the public API. It lives in
   `dev/` on purpose: the library ships the channel (`:event-tap`), the read
   surface (`explain-stuck`/`tasks-by-status`), and the verbs (`retry`/`abort`/
   …), and deliberately stays OUT of the recovery-policy judgment. This is one
   good judgment, written on that public surface, that you can lift into your own
   codebase and adapt. (Its exercised behavior lives in
   `test/dj/concurrency/reference_co_supervisor_test.clj`.)

   ## What a co-supervisor is for

   A supervised task that exhausts its retries PARKS — it will block `deref`
   forever until someone intervenes. A co-supervisor is the automated
   \"someone\": it reconciles the parked set on a cadence and drives recovery —
   retry with a per-task budget, then escalate. This is RECOVERY (after the
   fact); it is NOT pacing. Pacing — bounding concurrent load on a slow backend —
   is the pool cap's job (`:pool-caps`), and the two are kept separate on purpose:
   the cap prevents the pile-up up front, the co-sup mops up genuine failures.

   ## The cap-or-serialize contract (the load-bearing fact)

   Recovery retries can themselves stampede a slow backend — the classic
   thundering herd, reborn at the recovery layer. Something must pace them, and
   EITHER ONE ALONE suffices:

     * CAP THE POOL (recommended). Set `:pool-caps {your-pool W}` on the
       supervisor. A co-sup `retry` re-enters the SAME `scan-deadlines` admission
       gate as every other attempt, so it cannot out-run the cap. The co-sup can
       then be NAIVE — reconcile and retry every park immediately, with no
       serialization code at all. This is the headline recipe and the default
       mode here: pacing is one number.

     * SERIALIZE (fallback for an UNBOUNDED pool). If the pool has no cap, a naive
       loop WILL storm, so the co-sup must service one park at a time — retry,
       wait for it to settle, then take the next. Pass `:serialize? true`.

   Do NOT do both — serialization under a cap is redundant (proven: a capped
   naive loop and an unbounded serial loop both hold peak backend load at 1; only
   unbounded+naive storms). Prefer the cap. Both modes here use ONLY task status
   to detect settle — no backend introspection at all.

   ## What stays YOUR policy

   The library ships verbs, not verdicts. This reference makes three calls you
   should tune for your world: the reconcile CADENCE (`:poll-ms`), the per-task
   retry BUDGET (`:budget`), and what \"gave up\" MEANS (`:on-exhausted` — default
   `abort`; override to notify, switch provider, deliver a fallback, widen the
   cap, …). Optionally scope the whole loop to one `:pool` for per-pool recovery."
  (:require [dj.concurrency :as c]))

(def ^:private in-flight
  "Statuses that mean a task is progressing on its own. A co-sup retry we fired is
   'still being worked' while the task sits in one of these; once it leaves them
   it has either settled or re-parked."
  #{:queued :running :waiting-retry})

(defn- escalate!
  "The task's co-sup budget is spent: hand off to the operator's `:on-exhausted`
   hook, or `abort` by default. Passes the pure-data `explain-stuck` summary
   (`:error`/`:error-type`/`:pool`/`:age-ms`…), never a raw Throwable or closure."
  [sup summary on-exhausted]
  (if on-exhausted
    (on-exhausted sup (:task-id summary) summary)
    (c/abort sup (:task-id summary)
             (ex-info "co-supervisor budget spent"
                      {:type       :dj.concurrency/co-sup-gave-up
                       :task-id    (:task-id summary)
                       :last-error (:error summary)
                       :error-type (:error-type summary)}))))

(defn- eligible-parks
  "The parked summaries this loop should act on, oldest-first, optionally scoped
   to one `:pool`. Reads the authoritative, pure-data `explain-stuck` surface."
  [sup pool]
  (cond->> (:tasks (c/explain-stuck sup))
    pool (filter #(= pool (:pool %)))))

(defn- reconcile-naive!
  "One NON-BLOCKING pass: retry every eligible park (dedup against our own
   outstanding retries), escalate the budget-spent ones. Mutates `tries`/`pending`
   in place. Correct ONLY under a pool cap — the cap paces the retries this loop
   fires; without one it stampedes (see the ns docstring)."
  [sup {:keys [budget pool on-exhausted]} tries pending]
  ;; Drop from `pending` any task that has left the in-flight states: it either
  ;; settled (gone from the parked set) or re-parked (serviceable again).
  (swap! pending #(into #{} (filter (fn [id] (contains? in-flight (:status (c/task sup id))))) %))
  (let [parked (eligible-parks sup pool)]
    ;; Bound memory in a long-lived loop: forget tries for tasks no longer in play.
    (let [live (into @pending (map :task-id) parked)]
      (swap! tries #(select-keys % (seq live))))
    (doseq [{:keys [task-id] :as summary} parked]
      (when-not (contains? @pending task-id)
        (let [n (get @tries task-id 0)]
          (if (>= n budget)
            (do (escalate! sup summary on-exhausted)
                (swap! tries dissoc task-id))
            (do (swap! tries update task-id (fnil inc 0))
                (swap! pending conj task-id)
                (c/retry sup task-id))))))))          ;; fire — no wait

(defn- serve-one-serial!
  "Service the OLDEST eligible park to completion before returning: retry, wait
   for the retry to be PICKED UP (task leaves `:parked`), THEN wait for it to
   settle again. The pickup wait is load-bearing: a bare post-retry settle check
   races the un-processed `:repl/retry` (the task is still briefly `:parked`),
   returns 'settled' instantly, and never actually serializes. Status-only — no
   backend introspection. This hand-rolls the pacing a pool cap gives for free;
   use it only for an UNBOUNDED pool."
  [sup {:keys [budget pool on-exhausted]} tries running?]
  (when-let [{:keys [task-id] :as summary} (first (eligible-parks sup pool))]
    (let [n      (get @tries task-id 0)
          status #(:status (c/task sup task-id))]
      (if (>= n budget)
        (do (escalate! sup summary on-exhausted)
            (swap! tries dissoc task-id))
        (do (swap! tries update task-id (fnil inc 0))
            (c/retry sup task-id)
            ;; (1) wait for the retry to be picked up (leaves :parked) ...
            (while (and @running? (= :parked (status))) (Thread/sleep 5))
            ;; (2) ... then wait for it to settle (leaves the in-flight states).
            (while (and @running? (contains? in-flight (status))) (Thread/sleep 10)))))))

(defn start-co-supervisor
  "Start a reference co-supervisor on a virtual thread. Returns a 0-arg STOP fn.

   Options:
     :poll-ms      reconcile cadence in ms (default 50). Keep it comfortably
                   larger than the shell's retry-pickup latency.
     :budget       co-sup-level retries per task before `:on-exhausted` fires
                   (default 3). A fresh `retry` resets the task's own
                   `:max-attempts` budget; this is the SEPARATE how-many-times-
                   will-the-co-sup-try budget on top.
     :on-exhausted (fn [sup task-id summary]) run when a task's budget is spent;
                   default aborts the task. `summary` is the pure-data
                   `explain-stuck` entry.
     :pool         if set, only reconcile parks in this pool (per-pool recovery —
                   e.g. escalate `:llm-a` parks to a fallback, just retry `:db`).
     :serialize?   false (default): NAIVE — retry every park each pass; REQUIRES a
                   pool cap. true: service one park at a time, for an UNBOUNDED
                   pool. See the cap-or-serialize contract in the ns docstring.

   Recommended shape — cap the pool, run naive:

     (def sup  (c/create-supervisor {:pool-caps {:llm 4}}))
     (def stop (start-co-supervisor sup {:pool :llm :budget 3}))
     ;; … later …
     (stop)"
  [sup {:keys [poll-ms budget on-exhausted pool serialize?]
        :or   {poll-ms 50 budget 3}}]
  (let [running? (atom true)
        opts     {:budget budget :pool pool :on-exhausted on-exhausted}
        tries    (atom {})              ;; task-id -> co-sup retries issued (budget)
        pending  (atom #{})]            ;; task-ids with an outstanding naive retry
    (Thread/startVirtualThread
     (fn []
       (while @running?
         (try
           (if serialize?
             (serve-one-serial! sup opts tries running?)
             (reconcile-naive! sup opts tries pending))
           (catch Throwable t
             ;; one bad pass must never kill the loop; surface it and continue.
             (binding [*out* *err*]
               (println "[co-supervisor] reconcile pass threw, continuing:"
                        (ex-message t)))))
         (Thread/sleep (long poll-ms)))))
    (fn stop-co-supervisor [] (reset! running? false))))
