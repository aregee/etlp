(ns etlp.core-test
  (:require [clojure.test :refer :all]
            [willa.core :as w]
            [cheshire.core :as json]
            [etlp.core :as etlp :refer [build-message-topic
                                        create-kafka-stream create-kstream-processor logger]]))




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
                          [:created_at :timestamp
                           "NOT NULL" "DEFAULT CURRENT_TIMESTAMP"]]})

;; The config for our Kafka Streams app
(def kafka-config
  {"application.id" "random-etlp-kafka-stream"
   "bootstrap.servers" (or (System/getenv "BOOTSTRAP_SERVERS") "localhost:9092")
   "default.key.serde" "jackdaw.serdes.EdnSerde"
   "default.value.serde" "jackdaw.serdes.EdnSerde"
   "compression.type" "gzip"
   "max.request.size" "20971520"
   "num.stream.threads" (or (System/getenv "NUM_STREAM_THREADS") "16")
   "cache.max.bytes.buffering" "0"})


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

(defn add-msg-id [msg]
  (let [message-id (rand-int 10000)]
    [message-id msg]))

(defn- pipeline [params]
  (comp
  ;;  (map (partial merge (dissoc params :path)))
  ;;  (map logger)
   (filter valid-entry?)
   (keep transform-entry-if-relevant)))


(def pipeline-kstream
  (comp
   (map (fn [[_ load]]
          (:value load)))
   (filter valid-entry?)
   (keep transform-entry-if-relevant)
   (map add-msg-id)))


(defn- pg-pipeline [params]
  (comp
   (pipeline params)
   (partition-all 100)))


(defn- kafka-pipeline [params]
  (comp
  ;;  (pipeline params)
   (map add-msg-id)))


(def db-config-def {:id 1
                    :component :etlp.core/config
                    :ctx (merge {:name :db} db-config)})


(def kafka-config-def {:id 2
                       :component :etlp.core/config
                       :ctx (merge {:name :kafka} kafka-config)})

(def json-processor-def {:id 1
                         :component :etlp.core/processors
                         :ctx {:name :pg-processor
                               :type :pg-json-processor
                               :table-opts table-opts
                               :xform-provider pg-pipeline}})


(def topic-meta {:topic-name "kafka-json-message"
                 :partition-count 16
                 :replication-factor 1
                 :topic-config {"compression.type" "gzip"
                                "max.request.size" "20971520"}})

(def test-message-topic
  (build-message-topic {:topic-name "kafka-json-message"
                        :partition-count 16
                        :replication-factor 1
                        :topic-config {}}))



(def test-message-parsed-topic
  (build-message-topic {:topic-name "kafka-parsed"
                        :partition-count 16
                        :replication-factor 1
                        :topic-config {}}))


(def kafka-processor-def {:id 2
                          :component :etlp.core/processors
                          :ctx {:name :kafka-json-processor
                                :process-fn create-kafka-stream
                                :reducer :json-reducer
                                :topic test-message-topic
                                :xform-provider kafka-pipeline}})


(defn topology-builder
  "Takes topic metadata and returns a function that builds the topology."
  [topic-metadata topic-reducers]
  (let [entities {:topic/test-message (assoc (:test-message topic-metadata) ::w/entity-type :topic)
                  :topic/test-message-parsed (assoc (:test-message-parsed topic-metadata) ::w/entity-type :topic)
                  :stream/test-message {::w/entity-type :kstream
                                        ::w/xform pipeline-kstream}}
        ; We are good with this simple flow for now
        direct    [[:topic/test-message :stream/test-message]
                   [:stream/test-message :topic/test-message-parsed]]]

    {:workflow direct
     :entities entities
     :joins {}}))

(def kafka-stream-processor-def {:id 2
                                 :component :etlp.core/processors
                                 :ctx {:name :kafka-stream-processor
                                       :process-fn create-kstream-processor
                                       :topic-metadata   {:test-message test-message-topic
                                                          :test-message-parsed test-message-parsed-topic}
                                       :topology-builder topology-builder}})


;; (def etlp-app (etlp/init {:components [db-config-def json-processor-def]}))


;; (def pg-processor (etlp-app {:processor :pg-processor :params {:key 1}}))


;; (pg-processor {:path "resources/fix/" :days 1 :foo 24})


;; (def etlp-app (etlp/init {:components [kafka-config-def kafka-processor-def]}))

;; (def processor (etlp-app {:processor :kafka-json-processor :params {:key 1 :throttle 10000}}))

;; (processor {:path "resources/fix/" :days 1 :foo 24})


;; (def etlp-app (etlp/init {:components [kafka-config-def kafka-stream-processor-def]}))

;; (def stream-app (etlp-app {:processor :kafka-stream-processor :params {:key 1}}))