(defproject org.clojars.aregee/etlp "0.1.1"
  :description "Transducers based ETL processing pipeline"
  :url "https://github.com/aregee/etlp"
  :license {:name "MIT"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [ cheshire "5.10.0"]
                 [clj-postgresql "0.7.0"]
                 [org.clojure/core.async "0.4.500"]
                 [org.clojure/java.jdbc "0.7.11"]]
  :deploy-repositories {"releases" {:url "https://repo.clojars.org" :creds :gpg}} 
  :repl-options {:init-ns etlp.core})
