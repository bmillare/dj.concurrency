(ns dj.concurrency.store
  "Pluggable durable result store for task memoization.

   A task opts in with :dj.concurrency/durable-key in its context; a
   supervisor opts in with a :store satisfying ResultStore. Both absent
   => behavior identical to a supervisor without this feature.

   Keys are EDN data, stored VERBATIM. There is deliberately no hashing/
   digesting step: keeping the literal key in the journal keeps it
   introspectable (you can read a `results.edn` and see exactly which
   inputs produced which results). Clojure value-equality on the key data
   is what makes a re-run a hit, so map key order is irrelevant to
   equality. Derive your own keys from a task's inputs — e.g.
   `[:summarize prompt]`.")

(defprotocol ResultStore
  (lookup  [store k]
    "Returns an envelope map {:result r} on a hit, or nil on a miss.
     The envelope exists so nil/false are valid cached results.
     Must be cheap (in-memory read); called on worker virtual threads.")
  (record! [store k entry]
    "Durably persists entry ({:result r}) under k. MUST NOT return until
     the entry is durable (persist-then-publish ordering) — or throw.
     Called on worker virtual threads, so blocking here is acceptable.")
  (evict! [store k]
    "Removes the entry for k, durably. Used for REPL-driven invalidation."))

;; --- In-memory store (tests / non-durable memoization) ---

(defn atom-store
  "A ResultStore backed by a plain atom. Not durable; useful for tests
   and for pure in-process memoization."
  []
  (let [a (atom {})]
    (reify ResultStore
      (lookup  [_ k]       (get @a k))
      (record! [_ k entry] (swap! a assoc k entry) nil)
      (evict!  [_ k]       (swap! a dissoc k) nil))))
