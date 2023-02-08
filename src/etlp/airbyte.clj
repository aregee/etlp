(ns etlp.airbyte
  (:require [clojure.core.async :as a :refer [<!!]]
            [etlp.connector :as connector]))

;; (defprotocol AirbyteSource
;;   (spec [this])
;;   (check [this])
;;   (discover [this])
;;   (read [this]))

;; (defrecord AirbyteS3Source [s3-config bucket processors]
;;   AirbyteSource
;;   (spec [this] {:supported-destination-streams []
;;                 :supported-source-streams [{:stream_name "s3_stream"
;;                                             :schema {:type "object"
;;                                                      :properties {}}}]})
;;   (check [this] {:status "success"})
;;   (discover [this] {:streams [{:stream_name "s3_stream"
;;                                 :schema {:type "object"
;;                                          :properties {}}}]})
;;   (read [this] {})
;;   (start [this]
;;     (let [topology (atom (processing-topology this))
;;           etlp (connector/connect @topology)]
;;       (a/<!!
;;        (a/pipeline 6 (doto (a/chan) (a/close!))
;;                    (comp
;;                     (map (fn [d] (println d)))
;;                     (partition-all 10)
;;                     (keep save-into-database))
;;                    (get-in etlp [:processor-5 :channel])
;;                    (fn [ex]
;;                      (println (str "Execetion Caught" ex))))))))


(defprotocol EtlpSource
  (spec [this] "Return the spec of the source.")
  (check [this] "Check the validity of the source configuration.")
  (discover [this] "Discover the available schemas of the source.")
  (read [this] "Read data from the source and return a sequence of records."))

(defrecord EtlpS3Source [s3-config bucket processors topology-builder]
  EtlpSource

  (spec [this] {:supported-destination-streams []
                :supported-source-streams [{:stream_name "s3_stream"
                                            :schema {:type "object"
                                                     :properties {:s3-config {:type "object"
                                                                              :description "S3 connection configuration."}
                                                                  :bucket {:type "string"
                                                                           :description "The name of the S3 bucket."}
                                                                  :processors {:type "object"
                                                                               :description "Processors to be used to extract and transform data from the S3 bucket."}}}}]})

  (check [this]
    (let [errors (conj [] (when (nil? (:s3-config this))
                            "s3-config is missing")
                       (when (nil? (:bucket this))
                         "bucket is missing")
                       (when (nil? (:processors this))
                         "processors is missing"))]
      {:status (if (empty? errors) :valid :invalid)
       :message (if (empty? errors) "Source configuration is valid." (str "Source configuration is invalid. Errors: " (clojure.string/join ", " errors)))}))

  (discover [this] {:streams [{:stream_name "s3_stream"
                               :schema {:type "object"
                                        :properties {:data {:type "string"}}}}]})
  (read [this]
    (let [topology (topology-builder this)
          etlp (connector/connect topology)
          data-channel (get-in etlp [:processor-5 :channel])]
      (doall
       (a/<!!
        (a/pipeline 6 (doto (a/chan) (a/close!))
                    (comp
                     (map (fn [d] (println d))))
                    data-channel
                    (fn [ex]
                      (println (str "Execetion Caught" ex)))))))))