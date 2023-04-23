(ns etlp.connector.core
  (:require [clojure.core.async :as a]
            [clojure.pprint :refer [pprint]]))

(defmulti etlp-source (fn [op source] op))

(defmethod etlp-source :read [_ source]
  (.read! source))

(defmethod etlp-source :spec [_ source]
  (.spec source))

(defmethod etlp-source :check [_ source]
  (.check source))

(defmethod etlp-source :discover [_ source]
  (.discover source))


(defmulti etlp-destination (fn [op dest] op))

(defmethod etlp-destination :write [_ dest]
  (.write! dest))

(defmethod etlp-destination :spec [_ dest]
  (.spec dest))

(defmethod etlp-destination :check [_ dest]
  (.check dest))


(defprotocol EtlpConnection
  (spec [this] "Return the spec of the source.")
  (source [this] "Soruce Connector.")
  (destination [this] "Destination Connector")
  (stop [this])
  (start [this] "Trigger the A->B Flow using connector/connect"))

(defrecord EtlpConnect [config source destination xform pipeline-chan]
  EtlpConnection
  (spec [this])
  (source [this]
    (:source this))
  (destination [this]
    (:destination this))
  (start [this]
    (let [dest          (etlp-destination :write (:destination this))
          src           (etlp-source :read (:source this))
          src-output    (get-in src  [:etlp-output :channel])
          dest-input    (get-in dest [:etlp-input  :channel])
          dest-output   (get-in dest [:etlp-output :channel])
          threads       (get-in this [:config :threads])
          partitions    (get-in this [:config :partitions])
          xf            (:xform this)
          pipeline-chan (a/pipeline threads dest-input xf src-output)]
        (assoc this :pipeline-chan (a/merge [dest-output]))))
  (stop [this]
    (when-let [pipeline-chan (:pipeline-chan this)]
      (a/close! pipeline-chan)
      (assoc this :pipeline-chan nil))))


(defn connect [{:keys [source destination xform threads partitions] :as config}]
  (let [etlp-src   source
        etlp-dest  destination
        connection (map->EtlpConnect {:config      {:threads threads}
                                      :source      etlp-src
                                      :destination etlp-dest
                                      :xform       xform})]
    connection))
