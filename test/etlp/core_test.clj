(ns etlp.core-test
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.string :as s]
            [clojure.test :refer :all]
            [clojure.tools.logging :refer [debug]]
            [etlp.core :as etlp :refer [build-message-topic
                                        create-kafka-stream-processor
                                        create-kstream-topology-processor create-pg-stream-processor create-stdout-stream-processor]]
            [etlp.reducers :refer [lines-reducible]]
            [etlp.s3 :refer [s3-invoke]]
            [etlp.utils :refer [wrap-record]]
            [willa.core :as w]))

(def db-config
  {:host (System/getenv "DB_HOSTNAME")
   :user (System/getenv "DB_USER")
   :dbname (System/getenv "DB_NAME")
   :password (System/getenv "DB_PASSWORD")
   :port 5432})

(def kafka-config
  {"application.id" "multiple-etlp-kafka-stream"
   "bootstrap.servers" (or (System/getenv "BOOTSTRAP_SERVERS") "localhost:9092,localhost:9093,localhost:9094")
   "default.key.serde" "jackdaw.serdes.EdnSerde"
   "default.value.serde" "jackdaw.serdes.EdnSerde"
   "compression.type" "gzip"
   "max.request.size" "20971520"
   "num.stream.threads" (or (System/getenv "NUM_STREAM_THREADS") "1")
   "cache.max.bytes.buffering" "0"})

(def s3-config {:region "us-east-1"
                :credentials {:access-key-id (System/getenv "ACCESS_KEY_ID")
                              :secret-access-key (System/getenv "SECRET_ACCESS_KEY_ID")}})

(def table-opts {:table :test_log_clj
                 :specs  [[:id :serial "PRIMARY KEY"]
                          [:type :varchar]
                          [:field :varchar]
                          [:file :varchar]
                          [:key :varchar]
                          [:created_at :timestamp
                           "NOT NULL" "DEFAULT CURRENT_TIMESTAMP"]]})

(def table-hl7-opts {:table :test_hl7_log
                     :specs  [[:id :serial "PRIMARY KEY"]
                              [:etlp_raw :jsonb]
                              [:created_at :timestamp
                               "NOT NULL" "DEFAULT CURRENT_TIMESTAMP"]]})

(def test-message-topic
  (build-message-topic {:topic-name "kafka-json-message"
                        :partition-count 1
                        :replication-factor 1
                        :topic-config {}}))


(def test-message-raw-topic
  (build-message-topic {:topic-name "kafka-logs-all"
                        :partition-count 1
                        :replication-factor 1
                        :topic-config {}}))

(def test-message-parsed-topic
  (build-message-topic {:topic-name "kafka-logs-parsed"
                        :partition-count 1
                        :replication-factor 1
                        :topic-config {}}))



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

(defn- pipeline-hl7v2 [params]
  (comp
   (map wrap-record)
   (map (fn [recrd]
          {:etlp_raw (json/decode recrd)}))
   (partition-all 100)))

(defn- stdout-pipeline-hl7v2 [params]
  (comp
   (map (fn [segments] (s/join "\r" segments)))
   (map wrap-record)))

(defn- pipeline [params]
  (comp
  ;;  (map (fn [log] (pprint log) log))
   (filter valid-entry?)
   (keep transform-entry-if-relevant)))

(defn- pg-pipeline [params]
  (comp
   (pipeline params)
   (partition-all 100)))

(defn- kafka-pipeline [params]
  (comp
   (pipeline params)
   (map add-msg-id)))

(defn- kafka-raw-pipeline [params]
  (comp
   (map (fn [segments] (s/join "\r" segments)))
   (map wrap-record)
   (map (fn [recrd]
          {:etlp_raw (json/decode recrd)}))
   (map add-msg-id)))

(def etlp-db-config {:id 1
                     :component :etlp.core/config
                     :ctx (merge {:name :db} db-config)})

(def etlp-s3-config {:id 2
                     :component :etlp.core/config
                     :ctx (merge {:name :s3} s3-config)})

(def etlp-kafka-config {:id 3
                        :component :etlp.core/config
                        :ctx (merge {:name :kafka} kafka-config)})

(def etlp-s3-hl7-reducer {:id 9
                          :component :etlp.core/reducers
                          :ctx {:name :hl7-reducer-s3
                                :source-type :s3
                                :xform-provider (fn [filepath opts]
                                                  (hl7-xform {}))}})

(def etlp-pg-json-processor {:id 4
                             :component :etlp.core/processors
                             :ctx {:name :s3-pg-processor
                                   :source-type :s3
                                   :process-fn create-pg-stream-processor
                                   :reducer :hl7-reducer-s3
                                   :table-opts table-hl7-opts
                                   :xform-provider pipeline-hl7v2}})

(def etlp-stdout-hl7-processor {:id 8
                                :component :etlp.core/processors
                                :ctx {:name :s3-stdout-processor
                                      :source-type :s3
                                      :process-fn create-stdout-stream-processor
                                      :reducer :hl7-reducer-s3}})

(def etlp-s3-stdout-hl7-processor {:id 8
                                   :component :etlp.core/processors
                                   :ctx {:name :stdout-processor
                                         :source-type :s3
                                         :process-fn create-stdout-stream-processor
                                         :reducer :hl7-reducer-s3
                                         :xform-provider stdout-pipeline-hl7v2}})

(def etlp-fs-pg-json-processor {:id 5
                                :component :etlp.core/processors
                                :ctx {:name :fs-pg-processor
                                      :source-type :s3
                                      :process-fn create-pg-stream-processor
                                      :reducer :json-reducer
                                      :table-opts table-opts
                                      :xform-provider pg-pipeline}})
(def etlp-s3-kafka-processor {:id 6
                              :component :etlp.core/processors
                              :ctx {:name :s3-kafka-json-processor
                                    :source-type :s3
                                    :process-fn create-kafka-stream-processor
                                    :reducer :json-reducer-s3
                                    :topic test-message-topic
                                    :xform-provider (comp stdout-pipeline-hl7v2 kafka-raw-pipeline)}})
(def etlp-hl7-kafka-processor {:id 6
                               :component :etlp.core/processors
                               :ctx {:name :s3-kafka-hl7-processor
                                     :source-type :s3
                                     :process-fn create-kafka-stream-processor
                                     :reducer :hl7-reducer-s3
                                     :topic test-message-topic
                                     :xform-provider kafka-raw-pipeline}})
(def etlp-fs-kafka-processor {:id 7
                              :component :etlp.core/processors
                              :ctx {:name :fs-kafka-json-processor
                                    :process-fn create-kafka-stream-processor
                                    :reducer :json-reducer
                                    :topic test-message-raw-topic
                                    :xform-provider kafka-raw-pipeline}})
(defn topology-builder
  "Takes topic metadata and returns a function that builds the topology."
  [topic-metadata topic-reducers]
  (let [entities {:topic/test-message (assoc (:test-message topic-metadata) ::w/entity-type :topic)
                  :topic/test-message-parsed (assoc (:test-message-parsed topic-metadata) ::w/entity-type :topic)
                  :stream/test-message {::w/entity-type :kstream
                                        ::w/xform (comp (map (fn [[id msg]]
                                                               (msg :value)))
                                                        (kafka-pipeline {}))}}
        ; We are good with this simple flow for now
        direct    [[:topic/test-message :stream/test-message]
                   [:stream/test-message :topic/test-message-parsed]]]

    {:workflow direct
     :entities entities
     :joins {}}))


(def etlp-kafka-topology-processor {:id 8
                                    :component :etlp.core/processors
                                    :ctx {:name :kafka-stream-processor
                                          :process-fn create-kstream-topology-processor
                                          :topic-metadata   {:test-message test-message-raw-topic
                                                             :test-message-parsed test-message-parsed-topic}
                                          :topology-builder topology-builder}})


(def etlp-app (etlp/init {:components [etlp-db-config
                                       etlp-kafka-config
                                       etlp-s3-config
                                       etlp-s3-hl7-reducer
                                       etlp-pg-json-processor
                                       etlp-fs-pg-json-processor
                                       etlp-s3-kafka-processor
                                       etlp-fs-kafka-processor
                                       etlp-kafka-topology-processor
                                       etlp-stdout-hl7-processor
                                       etlp-s3-stdout-hl7-processor
                                       etlp-hl7-kafka-processor]}))



(deftest test-lines-reducible
  (let [test-file (java.io.File. "resources/test.txt")
        test-string "line1\nline2\nline3\n"
        test-reader (io/reader test-file)]
    (with-open [w (io/writer test-file)]
      (.write w test-string))
    (testing "lines-reducible processes all lines in the file"
      (let [result (transduce (map #(str "Processed: " %))
                              conj []
                              (lines-reducible test-reader))]
        (is (= ["Processed: line1" "Processed: line2" "Processed: line3"] result)))
      (testing "lines-reducible closes the reader after processing"
        (let [closed? (try (.ready test-reader)
                           (catch java.io.IOException e
                             (if (s/includes? (.getMessage e) "Stream closed")
                               true
                               false)))]
          (is (= closed? true)))))))

(def config-map {:client (s3-invoke s3-config)
                 :bucket "platform-dev-env"
                 :prefix "messages"
                 :xform-provider (fn [params]
                                   (comp
                                    (keep (fn [log] (log :Key)))))
                 :params {:source-type "DooMEternal"}})

;; (stream-files-from-s3-bucket config-map)


(comment
  (deftest pg-fs-test
    (testing "etlp/files-to-pg-processor should execute without error"
      (let [pg-processor (etlp-app {:processor :fs-pg-processor :params {:key 1}})]
        (is (= nil (pg-processor {:path "resources/fix/" :prefix "stormbreaker/json"})))))))

(comment
  (deftest kafka-fs-test
    (testing "etlp/files-to-kafka-processor should execute without error"
      (let [processor (etlp-app {:processor :fs-kafka-json-processor :params {:key 1 :throttle 100}})]
        (is (= nil (processor {:path "resources/fix/"})))))))

;; (deftest pg-s3-test
;;   (testing "etlp/files-to-pg-processor should execute without error"
;;     (let [pg-processor (etlp-app {:processor :stdout-processor :params {:key 1}})]
;;       (is (= nil (pg-processor {:bucket (System/getenv "ETLP_TEST_BUCKET") :prefix "stormbreaker/hl7"}))))))


(deftest kafka-s3-test
  (testing "etlp/files-to-kafka-processor should execute without error"
    (let [processor (etlp-app {:processor :s3-kafka-hl7-processor :params {:key 1 :throttle 10000}})]
    (is (= nil (processor {:bucket (System/getenv "ETLP_TEST_BUCKET") :prefix "stormbreaker/hl7"}))))))


;; (stream-app (etlp-app {:processor :kafka-stream-processor :params {:key 1}})


(comment
  (defn gen-files []
    (letfn [(rand-obj []
              (case (rand-int 3)
                0 {:type "string" :field (apply str (repeatedly 30 #(char (+ 33 (rand-int 90)))))}
                1 {:type "string" :field (apply str (repeatedly 30 #(char (+ 33 (rand-int 90)))))}
                2 {:type "empty"}))]
      (with-open [f (io/writer "resources/dummy.json")]
        (binding [*out* f]
          (dotimes [_ 100000]
            (println (json/encode (rand-obj)))))))))

(comment "Test cases Block"

         (def topic-meta {:topic-name "kafka-json-message"
                          :partition-count 16
                          :replication-factor 1
                          :topic-config {"compression.type" "gzip"
                                         "max.request.size" "20971520"}})

         (deftest e-to-e-test
           (testing "etlp/files-to-kafka-processor should execute without error"
             (let [processor (etlp-app {:processor :kafka-json-processor :params {:key 1 :throttle 100000}})]
               (is (= nil (processor {:path "resources/fix/" :days 1 :foo 24}))))))

         (deftest e-to-e-test-stream
           (testing "etlp/kafka-topology-processor should execute without error for given time"
             (let [stream-app (etlp-app {:processor :kafka-stream-processor :params {:key 1}})
                   what-is-the-answer-to-life (future
                                                (debug "[Future] started computation")
                                                (stream-app)
                                                (debug "[Future] completed computation")
                                                42)]
               (is (= 42  @what-is-the-answer-to-life))))))