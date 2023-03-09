(ns etlp.s3
  (:require [clojure.core.async :as a :refer [>! chan go pipe pipeline]]
            [clojure.core.async.impl.protocols :as impl]
            [clojure.tools.logging.readable :refer [debug info warn]]
            [cognitect.aws.client.api :as aws]
            [cognitect.aws.credentials :as credentials]
            [clojure.pprint :refer [pprint]]
            [etlp.connector :refer [connect]]
            [etlp.airbyte :refer [EtlpAirbyteSource]]
            [etlp.async :refer [process-parallel]] ;; [etlp.core-test :refer [hl7-xform]]
            [etlp.reducers :refer [lines-reducible]]
            [etlp.utils :refer [wrap-error wrap-log wrap-record]])
  (:import [java.io BufferedReader InputStreamReader]
           [clojure.core.async.impl.channels ManyToManyChannel])
  (:gen-class))



(defn s3-reducible [xf blob]
  ;; (print "Should be blob" blob)
  (if (instance? ManyToManyChannel blob)
    (throw (Exception. "Invalid Blob"))
    (try
      (eduction
       xf
       (-> blob
           :Body
           InputStreamReader.
           BufferedReader.
           lines-reducible))
      (catch Exception e
        (println (str "Error Processing" e))
        (throw e)))))

(comment
  (eduction
   (lines-reducible
    (BufferedReader.
     (InputStreamReader. (s3-reducible :s :Body))))))

(defn stream-file [{:keys [s3-client bucket Key]}]
  (aws/invoke s3-client {:op :GetObject :request {:Bucket bucket :Key Key}}))

(defn s3-read-lines [{:keys [Key s3-client bucket] :as s3blob}]
; Using this method would allow you to 
; treat each line as a transducible entity which can allow you to apply various 
; other operations that you want to apply to reach row  eg filter, transform, map, etc  
  (try
    (-> (stream-file s3blob)
        :Body
        InputStreamReader.
        BufferedReader.
        lines-reducible)
    (catch Exception e
      (debug e)
      (throw e))))

(defn s3-invoke [{:keys [region credentials] :as s3conf}]
  ; assert for aws keys validation
  (try (aws/client {:api :s3 :region region :credentials-provider (credentials/basic-credentials-provider credentials)})
       (catch Exception ex
         (debug ex)
         (throw ex))))

(defn list-objects-sync [client req]
  (let [request req]
    (-> client (aws/invoke request) (:Contents))))

(defn parallel-bucket-reducer [{:keys [pipeline s3-client s3-config]}]
  ;TODO: Take all s3 config map for aws/s3 client
  (debug s3-client)
  (fn [{:keys [bucket prefix]}]
    (try
      (process-parallel pipeline (list-objects-sync s3-client {:op :ListObjectsV2 :request {:Bucket bucket :Prefix prefix}}))
      (catch Exception e
        (debug e)
        (warn e)))))


(defn list-objects-pipeline [{:keys [client bucket prefix files-channel]}]
  (let [list-objects-request {:op :ListObjectsV2 :request {:Bucket bucket :Prefix prefix}}]
    (a/go (loop [marker nil]
            (let [response   (a/<! (aws/invoke-async client (assoc-in list-objects-request [:request :NextContinuationToken] marker)))
                  contents   (:Contents response)
                  new-marker (:NextContinuationToken response)]
              (println ">>>>>" (keys response))
              (doseq [file contents]
                (a/>! files-channel file))
              (if new-marker
                (recur new-marker)
                (a/close! files-channel))))
          files-channel)))


(def pf (.availableProcessors (Runtime/getRuntime)))

(defn get-object-pipeline-async [{:keys [client bucket files-channel output-channel]}]
  (a/pipeline-async pf
                    output-channel
                    (fn [acc res]
                      (a/go
                        (let [content (a/<! (aws/invoke-async
                                             client {:op      :GetObject
                                                     :request {:Bucket bucket :Key (acc :Key)}}))]
                          (a/>! res content)
                          (a/close! res))))
                    files-channel))




(defn stream-files-from-s3-bucket [{:keys [client bucket prefix xform-provider params]}])

(def bucket-stdout-reducer
  (fn [{:keys [pipeline s3-client s3-config]}]
    (fn [{:keys [bucket prefix]}]
      (stream-files-from-s3-bucket {:client s3-client
                                    :bucket bucket
                                    :prefix prefix
                                    :params s3-config
                                    :xform-provider (fn [opts] pipeline)}))))


(def list-s3-processor  (fn [data]
                          (list-objects-pipeline {:client        (data :s3-client)
                                                  :bucket        (data :bucket)
                                                  :files-channel (data :channel)
                                                  :prefix        (data :prefix)})
                          (data :channel)))

(def get-s3-objects (fn [data]
                      (let [reducer (data :reducer)
                            output (a/chan 1 (mapcat (partial s3-reducible reducer)))]
                        (get-object-pipeline-async {:client         (data :s3-client)
                                                    :bucket         (data :bucket)
                                                    :files-channel  (data :channel)
                                                    :output-channel output})
                        output)))

(def etlp-processor (fn [ch]
                      (if (instance? ManyToManyChannel ch)
                        ch
                        (ch :channel))))


(defn s3-process-topology [{:keys [s3-config prefix bucket processors reducers reducer]}]
  (let [s3-client (s3-invoke s3-config)
        entities  {:list-s3-objects {:s3-client s3-client
                                     :bucket    bucket
                                     :prefix    prefix
                                     :channel   (a/chan)
                                     :meta      {:entity-type :processor
                                                 :processor   (processors :list-s3-processor)}}

                   :get-s3-objects {:s3-client s3-client
                                    :bucket    bucket
                                    :reducer   (reducers reducer)
                                    :meta      {:entity-type :processor
                                                :processor   (processors :get-s3-objects)}}

                   :etlp-output {:channel (a/chan)
                                 :meta    {:entity-type :processor
                                           :processor   (processors :etlp-processor)}}}
        workflow [[:list-s3-objects :get-s3-objects]
                  [:get-s3-objects :etlp-output]]]

    {:entities entities
     :workflow workflow}))


(defn save-into-database [rows batch]
  (swap! rows + (count batch))
  (println (wrap-log (str "Total Count of Records:: " @rows))))


(defrecord EtlpAirbyteS3Source [s3-config prefix bucket processors topology-builder reducers reducer]
  EtlpAirbyteSource
  (spec [this] {:supported-destination-streams []
                :supported-source-streams      [{:stream_name "s3_stream"
                                                 :schema      {:type       "object"
                                                               :properties {:s3-config  {:type        "object"
                                                                                         :description "S3 connection configuration."}
                                                                            :bucket     {:type        "string"
                                                                                         :description "The name of the S3 bucket."}
                                                                            :processors {:type        "object"
                                                                                         :description "Processors to be used to extract and transform data from the S3 bucket."}}}}]})

  (check [this]
    (let [errors (conj [] (when (nil? (:s3-config this))
                            "s3-config is missing")
                       (when (nil? (:bucket this))
                         "bucket is missing")
                       (when (nil? (:processors this))
                         "processors is missing"))]
      {:status  (if (empty? errors) :valid :invalid)
       :message (if (empty? errors) "Source configuration is valid." (str "Source configuration is invalid. Errors: " (clojure.string/join ", " errors)))}))

  (discover [this]
            ;; TODO use config and topology to discover schema from mappings
    {:streams [{:stream_name "s3_stream"
                :schema      {:type       "object"
                              :properties {:data {:type "string"}}}}]})
  (read! [this]
    (let [topology     (topology-builder this)
          etlp         (connect topology)
          records      (atom 0)
          reducers     (get-in this [:reducers])
          xform        (get-in this [:reducer])
          data-channel (get-in etlp [:etlp-output :channel])]
     data-channel)))


(def create-s3-source! (fn [{:keys [s3-config bucket prefix reducers reducer] :as opts}]
                        (let [s3-connector (map->EtlpAirbyteS3Source {:s3-config        s3-config
                                                                      :prefix           prefix
                                                                      :bucket           bucket
                                                                      :processors       {:list-s3-processor list-s3-processor
                                                                                         :get-s3-objects    get-s3-objects
                                                                                         :etlp-processor    etlp-processor}
                                                                      :reducers         reducers
                                                                      :reducer          reducer
                                                                      :topology-builder s3-process-topology})]
                         s3-connector)))

