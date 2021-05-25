(defproject etlp "0.1.0-SNAPSHOT"
  :description "Transducers based ETL processing pipeline"
  :url "https://github.com/aregee/etlp"
  :license {:name "MIT"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [clj-postgresql "0.7.0"]
                 [org.clojure/core.async "0.4.500"]
                 [org.clojure/java.jdbc "0.7.11"]]
  :profiles {:dev {:dependencies [[ cheshire "5.10.0"]]}}
  :repl-options {:init-ns etlp.core})
