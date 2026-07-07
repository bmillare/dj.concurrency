(ns dj.concurrency.llm-demo
  "A runnable validation of dj.concurrency against the motivating problem:
   driving flaky LLM inference endpoints (transient errors, 429 rate limits)
   from the REPL without the happy-path code drowning in retry/throttle/try-catch
   plumbing.

   Run the whole narrative:

     clojure -M:repl -e \"(require 'dj.concurrency.llm-demo)(dj.concurrency.llm-demo/-main)\"

   Outcomes are SCRIPTED (per-request atoms) so the demo is deterministic — no
   randomness — and the output can be trusted as a validation artifact."
  (:require [dj.concurrency :as c]))

(defn- wait-for
  ([pred] (wait-for pred 4000))
  ([pred timeout-ms]
   (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
     (loop []
       (cond (pred) true
             (> (System/currentTimeMillis) deadline) false
             :else (do (Thread/sleep 10) (recur)))))))

(defn- say [& xs] (apply println ">>>" xs))

;; -----------------------------------------------------------------------------
;; A fake LLM inference endpoint.
;;
;; Each call pops the next scripted outcome from `script` (an atom holding a
;; vector of keywords). This stands in for a real HTTP call to e.g. an
;; Anthropic/OpenAI endpoint that may 429 or transiently fail.
;; -----------------------------------------------------------------------------

(defn fake-llm
  "Returns a 0-arg closure (what you'd hand to `c/submit`) that, on each
   invocation, performs the next scripted outcome:
     :ok        -> returns a canned completion string
     :transient -> throws a retryable error
     :rate      -> throws a 429 (supervisor-wide throttle, retry-after 800ms)"
  [prompt script]
  (fn []
    (let [outcome (or (first @script) :ok)]
      (swap! script (comp vec rest))
      (case outcome
        :ok        (str "completion for " (pr-str prompt))
        :transient (throw (ex-info "endpoint 503" {}))
        :rate      (throw (ex-info "rate limited" {:status 429 :retry-after 800}))))))

;; -----------------------------------------------------------------------------
;; Demo A — the happy path stays trivial
;; -----------------------------------------------------------------------------

(defn demo-happy-path [sup]
  (say "DEMO A: happy path — business logic just derefs, no try/catch")
  (let [f (c/submit sup {:prompt "summarize the meeting"}
                    (fake-llm "summarize the meeting" (atom [:ok])))
        ;; This is the entire "business logic". No retry loop, no error handling.
        summary (str "Report: " @f)]
    (say "got:" summary)
    summary))

;; -----------------------------------------------------------------------------
;; Demo B — transient failures and 429s are handled OUTSIDE the happy path
;; -----------------------------------------------------------------------------

(defn demo-auto-recovery [sup]
  (say "DEMO B: a flaky call fails twice (transient) then succeeds — automatically")
  (let [f (c/submit sup {:prompt "extract action items"}
                    (fake-llm "extract action items"
                              (atom [:transient :transient :ok])))]
    ;; Consumer still just derefs; the supervisor did 2 backed-off retries.
    (say "deref returned:" (deref f 8000 :timed-out))))

(defn demo-throttle-coordination [sup]
  (say "DEMO C: a 429 throttles the WHOLE supervisor; sibling requests queue, then drain")
  (let [r1 (c/submit sup {:prompt "req-1"} (fake-llm "req-1" (atom [:rate :ok])))
        throttled? #(seq (:pool-throttle (c/state sup)))]  ;; untagged => :default pool
    ;; Wait until the 429 has set the (default pool's) throttle window.
    (wait-for throttled?)
    (say "throttle active? " (boolean (throttled?)))
    (let [r2 (c/submit sup {:prompt "req-2"} (fake-llm "req-2" (atom [:ok])))
          r3 (c/submit sup {:prompt "req-3"} (fake-llm "req-3" (atom [:ok])))]
      ;; r2/r3 were submitted during the window -> they queue, not run.
      (Thread/sleep 50)
      (say "while throttled, sibling statuses:"
           (mapv #(:status (c/task %)) [r2 r3]))
      (say "all three eventually resolve:"
           (mapv #(deref % 8000 :timed-out) [r1 r2 r3])))))

;; -----------------------------------------------------------------------------
;; Demo D — the payoff: a stuck call PARKS, and you recover it from the REPL
;; -----------------------------------------------------------------------------

(defn demo-park-and-recover [sup]
  (say "DEMO D: endpoint stays down -> task parks -> recover from the REPL")
  (let [f (c/submit sup {:prompt "draft the release notes" ::c/max-attempts 1}
                    (fake-llm "draft the release notes" (atom [:transient])))]
    (wait-for #(= :parked (:status (c/task f))))
    (say "task status:" (:status (c/task f)) "— consumer is still blocked, not crashed")
    ;; At the REPL you can see exactly what was in flight, including its context:
    (say "parked context:" (:context (c/task f)))
    (say "the failing error:" (ex-message (:error (c/task f))))
    ;; Option 1: hand back a mocked result so the blocked consumer proceeds.
    (say "delivering a mock result to unblock the consumer...")
    (c/deliver-result f "MOCK: release notes v1")
    (say "consumer unblocked with:" (deref f 2000 :timed-out))))

(defn -main [& _]
  (let [sup (c/create-supervisor {:name "llm-demo"})]
    (try
      (demo-happy-path sup)            (println)
      (demo-auto-recovery sup)         (println)
      (demo-throttle-coordination sup) (println)
      (demo-park-and-recover sup)      (println)
      (say "DONE — all demos completed")
      (finally (c/stop! sup)))))
