(ns etlp.airbyte
  (:require [clojure.core.async :as a :refer [<!!]]
            [etlp.connector :as connector]))


(defprotocol EtlpAirbyteSource
  (spec [this] "Return the spec of the source.")
  (check [this] "Check the validity of the source configuration.")
  (discover [this] "Discover the available schemas of the source.")
  (read! [this] "Read data from the source and return a sequence of records."))



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
          reducers     (get-in this [:reducers])
          xform        (get-in this [:reducer])
          data-channel (get-in etlp [:etlp-output :channel])]
      (a/pipeline (.availableProcessors (Runtime/getRuntime)) (doto (a/chan) (a/close!))
                    (comp
                     (reducers xform)
                     (map (fn [d] (println d) d))
                     (partition-all 10)
                     (keep save-into-database))
                    data-channel
                    (fn [ex]
                      (println (str "Execetion Caught" ex)))))))
