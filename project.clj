(defproject com.github.aregee/etlp "0.4.1-SNAPSHOT"
  :description "Transducers based ETL processing pipeline"
  :url "https://github.com/aregee/etlp"
  :license {:name "MIT"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [cheshire "5.10.0"]
                 [clj-postgresql "0.7.0"]
                 [integrant "0.8.0"]
                 [willa "0.3.0"]
                 [org.apache.kafka/kafka-streams-test-utils "2.8.0"]
                 [com.health-samurai/jute "0.2.0-SNAPSHOT"]
                 [com.cognitect.aws/endpoints "1.1.12.380"]
                 [com.cognitect.aws/s3 "825.2.1250.0"]
                 [clj-http "3.12.3"]
                 [com.cognitect.aws/api "0.8.635"]
                 [org.clojure/tools.logging "1.2.4"]
                 [org.clojure/core.async "0.4.500"]
                 [com.github.seancorfield/honeysql "2.4.1002"]
                 [org.clojure/java.jdbc "0.7.11"]]
  :deploy-repositories {"releases" {:url "https://repo.clojars.org" :creds :auth}}
  :plugins [[lein-with-env-vars "0.2.0"]]
  :hooks [leiningen.with-env-vars/auto-inject]
  :profiles {:dev {:dependencies []}}
  :env-vars [".env-vars"]
  :repl-options {:init-ns etlp.core-test})
