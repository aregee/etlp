(ns etlp.core-test
  (:require [clojure.test :refer :all]
            [cheshire.core :as json]
            [clojure.java.io :as io]
            [etlp.core :as etlp]
            [etlp.reducers :as reducers]))




(defn gen-files []
    (letfn [(rand-obj []
      (case (rand-int 3)
        0 {:type "string" :field (apply str (repeatedly 30 #(char (+ 33 (rand-int 90))))) }
        1 {:type "string" :field (apply str (repeatedly 30 #(char (+ 33 (rand-int 90)))))}
        2 {:type "empty"}))]
      (with-open [f (io/writer "resources/dummy.json")]
        (binding [*out* f]
        (dotimes [_ 100000]
      (println (json/encode (rand-obj))))))))

(def db-config {:host "localhost"
                :user "postgres"
                :dbname "test"
                :password "postgres"
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

;; (def db (etlp/create-pg-connection db-config))

;; (def bulk-json-writer (etlp/create-pg-destination @db table-opts))

;; (defn- pipeline [params]
;;   (comp (mapcat etlp/json-reducer)   ;; Pipeline transducer
;;         (filter valid-entry?)
;;         (keep transform-entry-if-relevant)
;;         (partition-all 100)
;;         (map @bulk-json-writer)))

;; (def json-processor (etlp/create-pipeline-processor {:pg-connection @db :pipeline pipeline}))

;; (defn exec-fp [{:keys [path days]}]
;;   (json-processor {:params 1 :path path}))

;; (gen-files)


;; (def json-reducer-def {:id 1
;;                        :component :etlp.core/reducers
;;                        :ctx {:name :json-reducer
;;                              :xform-provider (fn [filepath opts]
;;                                                (comp (map reducers/parse-line)))}})

;; (def json-processor-def {:id 1
;;                          :component :etlp.core/pipelines
;;                          :ctx {:name :json-processor
;;                                :process-fn etlp/create-json-stream
;;                                :table-opts {}
;;                                :xform-provider (fn [ag] (comp (map prn)))}})

(def db-config-def {:id 1
                    :component :etlp.core/config 
                    :ctx (merge {:name :db} db-config)})


(def etlp-app (etlp/init {:components [db-config-def]}))
(def json-processor (get-in etlp-app [:etlp.core/processors :json-processor]))
;; (clojure.pprint/pprint json-processor)
(def processor (json-processor {:days 1}))
(processor {:path "resources/" :params 1})
;; (deftest e-to-e-test
;;   (testing "etlp/create-pipeline-processor should execute without error"
;;     (is (= true (exec-fp {:path "resources/" :days 1})))))
;; (deftest e-to-e-test
;;   (testing "etlp/create-pipeline-processor should execute without error"
;;     (is (= nil nil ))))