(ns etlp.core-test
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.core.async :as a]
            [clojure.string :as s]
            [etlp.core :as etlp]
            [etlp.utils.mapper :as mapper]
            [etlp.processors.stdin :refer [create-stdin-source!]]
            [etlp.processors.stdout :refer [create-stdout-destination!]]
            [clojure.test :refer :all]
            [etlp.utils.core :refer [wrap-record wrap-log]]
            [clojure.tools.logging :refer [debug]]))

(defn record-start? [log-line]
  (.startsWith log-line "MSH"))

(def invalid-msg? (complement record-start?))

(defn is-valid-hl7? [msg]
  (cond-> []
    (invalid-msg? msg) (conj "Message should start with MSH segment")
    (< (.length msg) 8) (conj "Message is too short (MSH truncated)")))

(defn next-log-record [ctx hl7-lines]
  (let [head (first hl7-lines)
        body (take-while (complement record-start?) (rest hl7-lines))]
    (remove nil? (conj body head))))

(defn hl7-xform
  "Returns a lazy sequence of lists like partition, but may include
  partitions with fewer than n items at the end.  Returns a stateful
  transducer when no collection is provided."
  ([ctx]
   (fn [rf]
     (let [a (java.util.ArrayList.)]
       (fn
         ([] (rf))
         ([result]
          (let [result (if (.isEmpty a)
                         result
                         (let [v (vec (.toArray a))]
                             ;;clear first!
                           (.clear a)
                           (unreduced (rf result v))))]
            (rf result)))
         ([result input]
          (.add a input)
          (if (and (> (count a) 1) (= true (record-start? input)))
            (let [v (vec (.toArray a))]
              (.clear a)
              (.add a (last v))
              (rf result (drop-last v)))
            result))))))

  ([ctx log-lines]
   (lazy-seq
    (when-let [s (seq log-lines)]
      (let [record (doall (next-log-record ctx s))]
        (cons record
              (hl7-xform ctx (nthrest s (count record)))))))))



(defn create-xcom-input-processor [{:keys [config mapper]}]
  (let [s3-source        {:threads    3
                          :partitions 100000
                          :reducers   {:json-reducer
                                       (comp
                                        (map (fn [input]
                                               (try
                                                 (json/decode input)
                                                 (catch Exception e
                                                   :etlp-invalid-json))))
                                        (filter (fn [decoded-json]
                                                  (not= decoded-json :etlp-invalid-json))))}
                          :reducer    :json-reducer}
        destination-conf {:threads    1
                          :partitions 100000}]

    {:source      (create-stdin-source! s3-source)
     :destination (create-stdout-destination! destination-conf)
     :xform       (comp
;                   (remove #(= % :etlp-stdin-eof))
                   (map wrap-record))
     :threads     3}))

(def xcom-processor {:name        :airflow-xcom-processor
                     :process-fn  create-xcom-input-processor
                     :etlp-config {}
                     :etlp-mapper {:base-url "http://localhost:3000"
                                   :specs    {:ADT-PL       "13"
                                              :test-mapping "16"}}})


(def xcom-connector {:id 2
                     :component :etlp.core/processors
                     :ctx xcom-processor})


;; (def etl-pipeline (etlp/init {:components [kafka-connector]}))


(def command {:processor :hl7-s3-kafka :params {:command :etlp.core/start
                                                :options {:foo :bar}}})


;; (def xcom-command {:processor :airflow-xcom-processor :params {:command :etlp.core/start

;; (etl-pipeline xcom-command)
