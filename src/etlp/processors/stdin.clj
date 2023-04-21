(ns etlp.processors.stdin
  (:require [clojure.core.async :as a]
            [etlp.connector.protocols :refer [EtlpSource]]))

(defn read-stdin [output-chan]
  (a/go-loop []
    (let [line (read-line)]
      (if (nil? line)
        (do
          (a/>! output-chan :etlp-stdin-eof)
          (a/close! output-chan))
        (do
          (a/>! output-chan line)
          (recur))))))

(comment
  (future
        (loop []
          (let [line (read-line)]
            (when line
              (a/>!! in-chan line)
              (recur))))))

(defrecord EtlpStdinSource [topology-builder processors reducers reducer threads partitions]
  EtlpSource
  (spec [this] {:supported-destination-streams []
                :supported-source-streams      [{:stream_name "stdin_stream"
                                                 :schema      {:type       "object"
                                                               :properties {:data {:type "string"}}}}]})

  (check [this]
    {:status  :valid
     :message "Source configuration is valid."})

  (discover [this]
    {:streams [{:stream_name "stdin_stream"
                :schema      {:type       "object"
                              :properties {:data {:type "string"}}}}]})

  (read! [this]
    (let [xform    (reducers reducer)
          in-chan (a/chan (a/buffer partitions) xform)
          _ (read-stdin in-chan)]
      {:etlp-output {:channel in-chan}})))

(defn create-stdin-source!
  [{:keys [topology-builder processors reducers reducer threads partitions] :as opts}]
  (let [stdin-connector (map->EtlpStdinSource {:topology-builder topology-builder
                                               :processors       processors
                                               :reducers         reducers
                                               :reducer          reducer
                                               :threads          threads
                                               :partitions       partitions})]
    stdin-connector))
