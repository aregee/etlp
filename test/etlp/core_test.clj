(ns etlp.core-test
  (:require [clojure.test :refer :all]
            [cheshire.core :as json]
            [clojure.java.io :as io]
            [etlp.core :as etlp]))


(letfn [(rand-obj []
  (case (rand-int 3)
    0 {:type "string" :field (apply str (repeatedly 30 #(char (+ 33 (rand-int 90))))) }
    1 {:type "string" :field (apply str (repeatedly 30 #(char (+ 33 (rand-int 90)))))}
    2 {:type "empty"}))]
  (with-open [f (io/writer "resources/fixtures/dummy.json")]
    (binding [*out* f]
    (dotimes [_ 100000]
    (println (json/encode (rand-obj)))))))

(def table-opts {
                 :db {:host "localhost"
                      :user "test"
                      :dbname "test"
                      :password "test"
                      :port 5432 }
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


(def write-json-logs (etlp/pg-destination (table-opts :table)))

(defn- pipeline [conn date]
  (comp (mapcat etlp/json-reducer)    ;; Pipeline transducer
        (filter valid-entry?)
        (keep transform-entry-if-relevant)
        (partition-all 1000)
        (map (write-json-logs conn))))

(def json-processor (etlp/create-pipeline-processor {:table-opts table-opts}))

(defn exec-fp [{:keys [path days]}]
  (json-processor {:pipeline pipeline :params 1 :path path}))


(deftest method-test-files
  (testing "etlp/files should return list of files in given directory"
    (is (= 1 (count (etlp/files "resources/fixtures/"))))))

(deftest method-test-with-path
  (testing "etlp/with-path should concat base-path and filename"
    (is (= "resources/fixtures/dummy.json" (etlp/with-path "resources/fixtures/" "dummy.json")))))

(deftest e-to-e-test
  (testing "etlp/create-pipeline-processor should execute without error"
    (is (= true (exec-fp {:path "resources/multi/" :days 1})))))
