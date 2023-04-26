(ns etlp.core-test
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.core.async :as a]
            [clojure.string :as s]
            [etlp.core :as etlp]
            [etlp.utils.mapper :as mapper]
            [etlp.processors.stdin :refer [create-stdin-source!]]
            [etlp.processors.stdout :refer [create-stdout-destination!]]
            [etlp.processors.db :refer [create-postgres-destination!]]
            [etlp.processors.kstream :refer [create-kstream-processor]]
            [etlp.processors.s3 :refer [create-s3-source!]]
            [jackdaw.serdes.edn :as serdes.edn]
            [clojure.test :refer :all]
            [willa.core :as w]
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

(def db-config
  {:host (System/getenv "DB_HOSTNAME")
   :user (System/getenv "DB_USER")
   :dbname (System/getenv "DB_NAME")
   :password (System/getenv "DB_PASSWORD")
   :port 5432})

(def table-opts {:table :test_hl7_etlp
                 :specs  [[:id :serial "PRIMARY KEY"]
                          [:type :varchar]
                          [:version :varchar]
                          [:source_stream :varchar]
                          [:schema :varchar]
                          [:timestamp :timestamp]
                          [:data :varchar]
                          [:created_at :timestamp
                           "NOT NULL" "DEFAULT CURRENT_TIMESTAMP"]]})

(defn create-hl7-processor [{:keys [config mapper]}]
  (let [s3-source {:s3-config (config :s3)
                   :bucket    (System/getenv "ETLP_TEST_BUCKET")
                   :prefix    "stormbreaker/small-hl7"
                   :threads   3
                   :partitions 100000
                   :reducers  {:hl7-reducer
                               (comp
                                (hl7-xform {})
                                (map (fn [segments]
                                       (s/join "\r" segments))))}
                   :reducer   :hl7-reducer}
        destination-conf {:pg-config (config :db)
                          :threads 1
                          :partitions 100000
                          :table (table-opts :table)
                          :specs (table-opts :specs)}]

    {:source (create-s3-source! s3-source)
     :destination (create-stdout-destination! destination-conf)
     :xform (comp
             (map wrap-record))
     :threads 3}))


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

(defn create-kstream-topology
  "Takes topic metadata and returns a function that builds the topology."
  [{:keys [config mapper options topics]}]
  (let [entities {:topic/etlp-input  (assoc (:etlp-input topics) ::w/entity-type :topic)
                  :topic/etlp-output (assoc (:etlp-output topics) ::w/entity-type :topic)
                  :stream/etlp-input {::w/entity-type :kstream
                                      ::w/xform       (comp (map (fn [[_ {:keys [id value]}]]
                                                                   [_ (wrap-record value)])))}}


        ; We are good with this simple flow for now
        workflow [[:topic/etlp-input :stream/etlp-input]
                  [:stream/etlp-input :topic/etlp-output]]]

    {:workflow workflow
     :entities entities
     :joins    {}}))

(def kafka-config
  {"application.id" "1-etlp-kafka-stream"
   "bootstrap.servers" (or (System/getenv "BOOTSTRAP_SERVERS") "localhost:9092,localhost:9093,localhost:9094")
   "default.key.serde" "jackdaw.serdes.EdnSerde"
   "default.value.serde" "jackdaw.serdes.EdnSerde"
   "compression.type" "gzip"
;   "max.request.size" "20971520"
   "num.stream.threads" (or (System/getenv "NUM_STREAM_THREADS") "1")
   "cache.max.bytes.buffering" "0"})

(def test-etlp-input
  {:topic-name         "hl7-message"
   :replication-factor 1
   :partition-count    1
   :key-serde          (serdes.edn/serde)
   :value-serde        (serdes.edn/serde)})


(def test-etlp-output
  {:topic-name         "output-topic"
   :replication-factor 1
   :partition-count    1
   :key-serde          (serdes.edn/serde)
   :value-serde        (serdes.edn/serde)})

(def topics-meta {:etlp-input test-etlp-input
                  :etlp-output test-etlp-output})

(def streaming-app (create-kstream-processor {:etlp-config {:kafka  kafka-config
                                                            :topics topics-meta}
                                              :options     {}
                                              :process-fn  create-kstream-topology
                                              :etlp-mapper {:base-url "http://192.168.1.101:3000"
                                                            :specs    {:ADT-PL       "13"
                                                                       :test-mapping "16"}}}))

(def s3-config {:region "us-east-1"
                :credentials {:access-key-id (System/getenv "ACCESS_KEY_ID")
                              :secret-access-key (System/getenv "SECRET_ACCESS_KEY_ID")}})

(def xcom-processor {:name :airflow-xcom-processor
                     :process-fn  create-xcom-input-processor
                     :etlp-config {}
                     :etlp-mapper {:base-url "http://localhost:3000"
                                   :specs    {:ADT-PL       "13"
                                              :test-mapping "16"}}})

(def hl7-processor {:name :airbyte-hl7-s3-connector
                    :process-fn  create-hl7-processor
                    :etlp-config {:s3 s3-config
                                  :db db-config}
                    :etlp-mapper {:base-url "http://localhost:3000"
                                  :specs    {:ADT-PL       "13"
                                             :test-mapping "16"}}})

(def airbyte-hl7-s3-connector {:id 1
                               :component :etlp.core/processors
                               :ctx hl7-processor})

(def xcom-connector {:id 2
                     :component :etlp.core/processors
                     :ctx xcom-processor})


;(def etl-pipeline (etlp/init {:components [airbyte-hl7-s3-connector xcom-connector]}))


(def command {:processor :airbyte-hl7-s3-connector :params {:command :etlp.core/start
                                                            :options {:foo :bar}}})


(def xcom-command {:processor :airflow-xcom-processor :params {:command :etlp.core/start
                                                               :options {:read :xcom}}})

;; (etl-pipeline xcom-command)
