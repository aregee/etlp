(ns etlp.core-test
  (:require [clojure.test :refer :all]
            [cheshire.core :as json]
            [clojure.java.io :as io]
            [etlp.core :as etlp]))




(defn gen-files []
    (letfn [(rand-obj []
      (case (rand-int 3)
        0 {:type "string" :field (apply str (repeatedly 30 #(char (+ 33 (rand-int 90))))) }
        1 {:type "string" :field (apply str (repeatedly 30 #(char (+ 33 (rand-int 90)))))}
        2 {:type "empty"}))]
      (with-open [f (io/writer "resources/fixtures/dummy.json")]
        (binding [*out* f]
        (dotimes [_ 100000]
      (println (json/encode (rand-obj))))))))

(def db-config {:host "localhost"
                :user "test"
                :dbname "test"
                :password "test"
                :port 5432})

(def table-opts {
                 :table :test_log_clj
                 :specs  [[:id :serial "PRIMARY KEY"]
                          [:type :varchar]
                          [:field :varchar]
                          [:created_at :timestamp
                           "NOT NULL" "DEFAULT CURRENT_TIMESTAMP"]]})

(defn valid-entry? [log-entry]
  (not= (:type log-entry) "empty"))

(defn transform-entry-if-relevant [log-entry]
  (cond (= (:type log-entry) "number")
    (let [number (:number log-entry)]
      (when (> number 900)
        (assoc log-entry :number (Math/log number))))

        (= (:type log-entry) "string")
        (let [string (:field log-entry)]
        (when (re-find #"a" string)
          (update log-entry :field str "-improved!")))))

(def db (etlp/create-pg-connection db-config))

(def bulk-json-writer (etlp/create-pg-destination @db table-opts))

(defn- pipeline [params]
  (comp (mapcat etlp/json-reducer)   ;; Pipeline transducer
        (filter valid-entry?)
        (keep transform-entry-if-relevant)
        (partition-all 1000)
        (map @bulk-json-writer)))

(def json-processor (etlp/create-pipeline-processor {:pg-connection @db :pipeline pipeline}))

(defn exec-fp [{:keys [path days]}]
  (json-processor {:params 1 :path path}))

(deftest e-to-e-test
  (testing "etlp/create-pipeline-processor should execute without error"
    (is (= true true))))