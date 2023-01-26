(ns etlp.connector
  (:require [clojure.core.async :as a :refer [<! >! chan go pipe pipeline]]
            [cognitect.aws.api :as aws]
            [clojure.string :as string]
            [etlp.s3 :refer [s3-reducible]]
            [etlp.utils :refer [wrap-log wrap-record]]
            [etlp.dag-builder :as dag]
            [integrant.core :as ig]))


(defrecord EtlpConfig [{:keys [etlp-config]}])
(defrecord EtlpTopic [config files-channel])
(defrecord EtlpStream [config xform-provider])
(defrecord Etlp [config])
(defrecord App [etlp-config etlp-topic etlp-stream etlp])

;; pipeline functions
(defn list-objects-pipeline [{:keys [client bucket prefix]}]
  (let [list-objects-request {:op :ListObjects :request {:Bucket bucket :Prefix prefix}}
        files-channel (chan 1)]
    (go (loop [marker nil]
          (let [response (<! (aws/invoke-async client list-objects-request))
                contents (:Contents response)
                new-marker (:NextMarker response)]
            (doseq [file contents]
              (>! files-channel file))
            (if new-marker
              (recur new-marker)
              (a/close! files-channel))))
        files-channel)))

(defn get-object-pipeline [{:keys [client bucket files-channel output-channel error-channel]}]
  (pipeline 1 output-channel (fn [file]
                               (let [get-object-request
                                     {:op :GetObject :request {:Bucket bucket :Key (:Key file)}}]
                                 (aws/invoke-async client get-object-request)))
            (fn [input-stream]
              (if (:error input-stream)
                (go (>! error-channel (:error input-stream)))
                input-stream))
            files-channel))



(defn stream-to-stdout [{:keys [client bucket stream-channel error-channel]}]
  (pipeline 1 (doto (a/chan) (a/close!))
            (comp
             (map wrap-log)
             (keep (fn [l] (println l) l))
             (partition-all 10))
            stream-channel))

;; integrant key methods
(defmethod ig/init-key :etlp-config [_ _] (-> EtlpConfig :etlp-config))

(defmethod ig/init-key :topology-builder [_ topology]
  (let [dag-builder (dag/build-dag topology)]
    (-> dag/DAGBuilder dag-builder)))

(defmethod ig/init-key :etlp-topic [_ {:keys [topology-builder] :as app}]
  (let [topology (:topology topology-builder)
        entities (:entities topology)
        topic-key (first (filter (fn [[from-node to-node]] (and (string/starts-with? from-node "topic/") (not (string/starts-with? to-node "topic/")))) (:workflow topology)))
        topic-config (get entities topic-key)
        files-channel ((:processors topic-config) (:etlp-config app))]
    (-> EtlpTopic topic-config files-channel)))

(defmethod ig/init-key :etlp-stream [_ {:keys [topology-builder etlp-topic]}]
  (let [topology (:topology topology-builder) entities (:entities topology)
        topic-key (first (filter (fn [[from-node to-node]] (and (string/starts-with? from-node "topic/") (not (string/starts-with? to-node "topic/")))) (:workflow topology)))
        stream-key (first (filter (fn [[from-node to-node]] (and (string/starts-with? from-node topic-key) (not (string/starts-with? to-node "topic/")))) (:workflow topology)))
        stream-config (get entities stream-key)
        xform-provider ((:xform stream-config))]
    (-> EtlpStream etlp-topic xform-provider)))

(defmethod ig/init-key :etlp [_ {:keys [topology-builder etlp-config]}]
  (let [topology (:topology topology-builder)
        entities (:entities topology)
        stream-key (first (filter (fn [[from-node to-node]] (and (string/starts-with? from-node "stream/") (not (string/starts-with? to-node "stream/")))) (:workflow topology)))
        etlp-key (first (filter (fn [[from-node to-node]] (and (string/starts-with? from-node stream-key) (not (string/starts-with? to-node "stream/")))) (:workflow topology)))
        etlp-config (get entities etlp-key)]
    (-> Etlp etlp-config)))

(defmethod ig/init-key :app [_ {:keys [etlp-config etlp-topic etlp-stream etlp topology-builder]}]
  (let [dag (:dag (:dag-builder topology-builder))]
    (dag/traverse-dag dag)
    (->App etlp-config etlp-topic etlp-stream etlp)))




;; TODO : adapt and test if we can use the topology below to compose our code above and test
; out all the functionality 

(comment



  (def topology
    {:entities {:topic/list-objects-pipeline {:processors list-objects-pipeline
                                              :entity-type :channel}
                :stream/list-objects {:entity-type :xform-provider
                                      :xform (fn [params]
                                               (comp
                                                (map wrap-log)))}

                :topic/get-object-pipeline {:processors get-object-pipeline
                                            :entity-type :channel}

                :stream/parse-file-formats {:entity-type :xform-provider
                                            :xform s3-reducible}
                :topic/etlp-procesoor {:processors stream-to-stdout
                                       :entity-type :channel}}


     :workflow [[:topic/list-objects-pipeline :stream/list-objects]
                [:stream/list-objects :topic/get-object-pipeline]
                [:topic/get-object-pipeline :stream/parse-file-formats]
                [:stream/parse-file-formats :topic/etlp-processor]]})


  (def config {:config {:client (aws/client {:api :s3}) :bucket "my-bucket" :prefix "path/to/files/"}
               :topic {}
               :stream {}
               :topology {}
               :etlp {}
               :app {}})


  (def app (ig/init config))

  (defn -main [& args]
    (let [app (ig/init config)]
      (println "ETLP process started"))))