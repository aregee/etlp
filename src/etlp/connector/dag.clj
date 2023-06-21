(ns etlp.connector.dag
  (:require [clojure.core.async :as a]
            [clojure.tools.logging :refer [debug warn info]]
            [clojure.pprint :refer [pprint]])
  (:import [clojure.core.async.impl.channels ManyToManyChannel]))

(defn- entity-type [entity]
  (-> entity :meta :entity-type))

(defn- xform-provider [xform-provider-name]

  (-> xform-provider-name :meta :xform))

(defn- get-threads [node-meta]

  (-> node-meta :meta :threads))

(defn- chan-provider? [node]

  (contains? node :channel-fn))

(defn- get-partitions [node-meta]

  (-> node-meta :meta :partitions))

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
        (warn (str "Raise Error :: " ex))
        (debug (.getMessage ex))))))

(defn- process-xform!! [node-data node-channel]
  (try
    (if (instance? ManyToManyChannel node-channel)
       (let [xform (xform-provider node-data)
              output-channel (a/chan (a/buffer (get-partitions node-data)))]
          (a/pipeline-blocking (get-threads node-data) output-channel xform node-channel)
          output-channel)
      node-channel)
    (catch Exception ex (println (str "Exception Occured" ex)))))

(defn- process-xform [node-data node-channel]
  (try
    (if (instance? ManyToManyChannel node-channel)
      (let [xform          (xform-provider node-data)
            output-channel (a/chan (a/buffer (get-partitions node-data)) xform)]
          (debug  ">>Sinvoked><>>here node_data" node-channel)
          (a/pipe node-channel output-channel))
      node-channel)
    (catch NullPointerException ex
      (warn (str "Etlp Exception:: " ex))
      (debug ">>> " ex))))

(defn build [topology]
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
                    output-channel (process-xform to-node output-channel)]
                (swap! entities assoc-in [to-entity :channel] output-channel))
              (swap! entities assoc-in [to-entity :channel] output-channel))))
        (if (xform-provider? from-node)
          (let [xform          (xform-provider from-node)
                output-channel (process-xform from-node from-node-data)]
            (if (processor? to-node)
              (let [output-channel (process-data output-channel to-node)]
                (swap! entities assoc-in [to-entity :channel]  output-channel))
              (swap! entities assoc-in [to-entity :channel] output-channel))))))
    @entities))
