(ns user
  "Dev REPL entry point. Keep the JVM alive; reload in place, don't restart.
   `dev` is on the classpath via the :nrepl / :repl aliases, so this ns is
   auto-loaded when the nREPL server boots."
  (:require [dj.concurrency :as c]))

(comment
  ;; Reload the library after editing a source file:
  (require '[dj.concurrency :as c] :reload)
  (require '[dj.concurrency.policy :as policy] :reload)
  (require '[dj.concurrency.shell :as shell] :reload)

  ;; Run the flaky-LLM-endpoint demo (dev/dj/concurrency/llm_demo.clj):
  (require '[dj.concurrency.llm-demo :as demo] :reload)
  )
