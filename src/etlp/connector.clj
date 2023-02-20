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
      (let [output-channel (a/chan 100)]
        (a/pipeline 16 output-channel xform input-channel)
        output-channel)
      input-channel)
    (catch Exception ex (println (str "Eexception Occured" ex)))))

(defn connect [topology]
  (let [workflow (:workflow topology)
        entities (atom (:entities topology))]
    (doseq [edge workflow]
      (let [[from-entity to-entity] edge
            from-node-data          (get @entities from-entity)
            to-node                 (get @entities to-entity)
            from-node               (get @entities from-entity)]
        (if (processor? from-node)
          (let [output-channel (process-data from-node-data from-node)]
            (if (xform-provider? to-node)
              (let [xform          (xform-provider to-node)
                    output-channel (process-xform xform output-channel)]
                (swap! entities assoc-in [to-entity :channel] output-channel))
              (swap! entities assoc-in [to-entity :channel] output-channel))))
        (if (xform-provider? from-node)
          (let [xform          (xform-provider from-node)
                output-channel (process-xform xform from-node-data)]
            (if (processor? to-node)
              (let [output-channel (process-data output-channel to-node)]
                (swap! entities assoc-in [to-entity :channel]  output-channel))
              (swap! entities assoc-in [to-entity :channel] output-channel))))))
    @entities))

(defprotocol EtlpConnection
  (spec [this] "Return the spec of the source.")
  (source [this] "Soruce Connector.")
  (destination [this] "Destination Connector")
  (start [this] "Trigger the A->B Flow using connector/connect"))
