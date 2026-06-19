(ns build
  "Build + publish tasks for dj.concurrency.

  Common commands (run inside the nix dev shell):
    clojure -T:build jar          ; build target/dj.concurrency-<version>.jar + pom
    clojure -T:build install      ; install that jar into your local ~/.m2
    clojure -T:build deploy       ; push the jar to Clojars
    clojure -T:build clean        ; remove target/

  Deploying needs Clojars credentials in the environment:
    CLOJARS_USERNAME = your Clojars username (bmillare)
    CLOJARS_PASSWORD = a Clojars *deploy token* (not your account password)

  Bump `version` below for each release. While the API is unstable we ship
  alpha builds (e.g. 0.1.0-alpha1, 0.1.0-alpha2, ...)."
  (:require [clojure.tools.build.api :as b]
            [deps-deploy.deps-deploy :as dd]))

;; net.clojars.bmillare is auto-verified for the Clojars user `bmillare` (no
;; DNS / GitHub verification needed). See the Verified-Group-Names policy.
(def lib 'net.clojars.bmillare/dj.concurrency)
(def version "0.1.0-alpha1")

(def class-dir "target/classes")
(def jar-file (format "target/%s-%s.jar" (name lib) version))

;; dj.concurrency is intentionally dependency-free and does NOT pin a Clojure
;; version (the consumer picks one). Skipping :root/:user keeps the implicit
;; default org.clojure/clojure dep out of the generated pom, so the published
;; pom has zero <dependencies> — matching the library's design.
(defn- basis [] (b/create-basis {:project "deps.edn" :root nil :user nil}))

(defn clean [_]
  (b/delete {:path "target"}))

(defn jar [_]
  (b/write-pom {:class-dir class-dir
                :lib lib
                :version version
                :basis (basis)
                :src-dirs ["src"]
                :scm {:url "https://github.com/bmillare/dj.concurrency"
                      :connection "scm:git:git://github.com/bmillare/dj.concurrency.git"
                      :developerConnection "scm:git:ssh://git@github.com/bmillare/dj.concurrency.git"
                      :tag (str "v" version)}
                :pom-data [[:description
                            "Smarter, manageable futures for Clojure: supervised async work with retries, throttling, and REPL-driven recovery."]
                           [:url "https://github.com/bmillare/dj.concurrency"]
                           [:licenses
                            [:license
                             [:name "Eclipse Public License 2.0"]
                             [:url "https://www.eclipse.org/legal/epl-2.0/"]]]]})
  (b/copy-dir {:src-dirs ["src"]
               :target-dir class-dir})
  (b/jar {:class-dir class-dir
          :jar-file jar-file}))

(defn install [_]
  (jar nil)
  (b/install {:basis (basis)
              :lib lib
              :version version
              :jar-file jar-file
              :class-dir class-dir}))

(defn deploy [_]
  (jar nil)
  (dd/deploy {:installer :remote
              :artifact (b/resolve-path jar-file)
              :pom-file (b/pom-path {:lib lib :class-dir class-dir})
              :sign-releases? false}))
