(ns etlp.connector
  (:require [clojure.core.async :as a]
            [clojure.pprint :refer [pprint]]
            [etlp.async :refer [save-into-database]])
  (:import [clojure.core.async.impl.channels ManyToManyChannel]))

(defn- entity-type [entity]
  (-> entity :meta :entity-type))

(defn- xform-provider [xform-provider-name]

  (-> xform-provider-name :meta :xform))

(defn- processors [processor-name]

  (-> processor-name :meta :processor))

(defn- xform-provider? [entity]

  (= :xform-provider (entity-type entity)))

(defn- processor? [entity]

  (= :processor (entity-type entity)))

(defn- process-data [data entity]
  (let [process-fn (processors entity)]
    (try
      (process-fn data)
      (catch Exception ex
        (println "Exception :: " ex)))))

(defn- process-xform [xform input-channel]
  (try
    (if (instance? ManyToManyChannel input-channel)
      (let [output-channel (a/chan 100000 xform)]
        (a/pipe input-channel output-channel))
      (let [output-channel (a/chan 100000)]
        (a/pipe (input-channel :channel) output-channel)))
    (catch Exception ex (println (str "Eexception Occured" ex)))))

(defn connect [topology]
  (let [workflow (:workflow topology)
        entities (atom (:entities topology))]
    (doseq [edge workflow]
      (let [[from-entity to-entity] edge
            from-node-data (get @entities from-entity)
            to-node (get @entities to-entity)
            from-node (get @entities from-entity)]
        (if (processor? from-node)
          (let [output-channel (process-data from-node-data from-node)]
            (if (xform-provider? to-node)
              (let [xform (xform-provider to-node)
                    output-channel (process-xform xform output-channel)]
                (swap! entities assoc-in [to-entity :channel] output-channel))
              (swap! entities assoc-in [to-entity :channel] output-channel))))
        (if (xform-provider? from-node)
          (let [xform (xform-provider from-node)
                output-channel (process-xform xform from-node-data)]
            (if (processor? to-node)
              (let [output-channel (process-data output-channel to-node)]
                (swap! entities assoc-in [to-entity :channel]  output-channel))
              (swap! entities assoc-in [to-entity :channel] output-channel))))))
    @entities))


(defprotocol EtlpConnector
  (spec [this] "Return the spec of the source.")
  (check [this] "Check the validity of the source configuration.")
  (discover [this] "Discover the available schemas of the source.")
  (read! [this] "Read data from the source and return a sequence of records."))



(defrecord EtlpS3Connector [s3-config prefix bucket processors topology-builder]
  EtlpConnector
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

  (discover [this]
            ;; TODO use config and topology to discover schema from mappings
    {:streams [{:stream_name "s3_stream"
                :schema {:type "object"
                         :properties {:data {:type "string"}}}}]})
  (read! [this]
    (let [topology (topology-builder this)
          etlp (connect topology)
          data-channel (get-in etlp [:processor-5 :channel])]
      (a/pipeline 1 (doto (a/chan) (a/close!))
                    (comp
                     (map (fn [d] (println d) d))
                     (partition-all 100)
                     (keep save-into-database))
                    data-channel
                    (fn [ex]
                      (println (str "Execetion Caught" ex)))))))
