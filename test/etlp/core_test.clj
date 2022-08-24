(ns etlp.core-test
  (:require [clojure.test :refer :all]
            [etlp.core :as etlp :refer [logger]]))




;; (defn gen-files []
;;   (letfn [(rand-obj []
;;             (case (rand-int 3)
;;               0 {:type "string" :field (apply str (repeatedly 30 #(char (+ 33 (rand-int 90)))))}
;;               1 {:type "string" :field (apply str (repeatedly 30 #(char (+ 33 (rand-int 90)))))}
;;               2 {:type "empty"}))]
;;     (with-open [f (io/writer "resources/dummy.json")]
;;       (binding [*out* f]
;;         (dotimes [_ 100000]
;;           (println (json/encode (rand-obj))))))))

(def db-config {:host "localhost"
                :user "postgres"
                :dbname "test"
                :password "postgres"
                :port 5432})

(def table-opts {:table :test_log_clj
                 :specs  [[:id :serial "PRIMARY KEY"]
                          [:type :varchar]
                          [:field :varchar]
                          [:file :varchar]
                          [:foo :varchar]
                          [:days :varchar]
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

(defn- pipeline [params]
  (comp
   (map (partial merge (dissoc params :path)))
   (map logger)
   (filter valid-entry?)
   (keep transform-entry-if-relevant)
   (partition-all 100)))

(def db-config-def {:id 1
                    :component :etlp.core/config
                    :ctx (merge {:name :db} db-config)})

(def json-processor-def {:id 1
                         :component :etlp.core/processors
                         :ctx {:name :local-processor
                               :type :json-processor
                               :table-opts table-opts
                               :xform-provider pipeline}})

(def etlp-app (etlp/init {:components [db-config-def json-processor-def]}))

(def json-processor (get-in etlp-app [:etlp.core/processors :local-processor]))

(def processor (json-processor {:key 1}))

(processor {:path "resources/fix/" :days 1 :foo 24})
