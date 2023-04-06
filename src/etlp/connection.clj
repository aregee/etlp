(ns etlp.connection
  (:require [clojure.core.async :as a]
            [clojure.pprint :refer [pprint]]
            [etlp.utils :refer [wrap-log]]
            [etlp.async :refer [save-into-database]])
  (:import [clojure.core.async.impl.channels ManyToManyChannel]))

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
  (start [this] "Trigger the A->B Flow using connector/connect"))

(defrecord EtlpConnect [config source destination xform]
  EtlpConnection
  (spec [this])
  (source [this]
    (:source this))
  (destination [this]
    (:destination this))
  (start [this]
    (let [dest (etlp-destination :write (:destination this))
          src  (etlp-source :read (:source this))
          xf   (:xform this)
          axfd (a/pipe src (a/chan (a/sliding-buffer 2000000) xf))]
      (a/pipe axfd dest))))

(defn create-connection [{:keys [source destination xform] :as config}]
  (let [etlp-src     source
        etlp-dest    destination
        connection (map->EtlpConnect {:config {:pf 1} :source etlp-src :destination etlp-dest :xform xform})]
    connection))
