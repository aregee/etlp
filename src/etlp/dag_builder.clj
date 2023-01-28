(ns etlp.dag-builder
  (:require [integrant.core :as ig]
            [clojure.core.async :as a]))

;; Define a record to hold the DAG data structure
(defrecord DagBuilder [dag])

;; Build the DAG using the topology information
(defn build-dag [topology]
  (let [entities (:entities topology)
        workflow (:workflow topology)
        graph (atom {})]
    (doseq [edge workflow]
      (let [[from-entity to-entity] edge
            from-node-edges (get graph from-entity)
            to-node-edges (get graph to-entity)]
        (if (nil? from-node-edges)
          (swap! graph assoc from-entity #{to-entity}))
        (if (not (nil? from-node-edges))
          (swap! graph assoc from-entity (conj from-node-edges to-entity)))))
    graph))

;; Define the init-key method for the DagBuilder component
(defmethod ig/init-key :dag-builder [_ topology]
  (let [dag (build-dag topology)]
    (-> DagBuilder dag)))

;; Define the topology-builder component
(defrecord TopologyBuilder [dag-builder])

;; Define the init-key method for the TopologyBuilder component
(defmethod ig/init-key :topology-builder [_ dag-builder]
  (-> TopologyBuilder dag-builder))

;; Define a function to traverse the DAG and create the processing workflow pipeline
(defn traverse-dag [dag app-config]
  (let [entities (:entities dag)
        workflow (:workflow dag)
        channels (atom {})]
    (doseq [edge workflow]
      (let [[from-node to-node] edge
            from-channel (get @channels from-node)
            from-channel-type (:entity-type (get entities from-node))
            to-channel (get @channels to-node)
            to-channel-type (:entity-type (get entities to-node))
            from-fn (:processors (get entities from-node))
            xform-fn (:xform-provider (get entities to-node))]

        (if (and (nil? from-channel) (not= :xform-provider from-channel-type))
          (let [from-channel (from-fn (:etlp-config app-config))]
            (swap! channels assoc from-node from-channel)))

        (if (and (nil? to-channel) (not= :xform-provider to-channel-type))
          (let [to-channel (a/chan 1)]
            (swap! channels assoc to-node to-channel)))

        (if (and from-channel to-channel (not= :xform-provider from-channel-type) (not= :xform-provider to-channel-type))
          (a/pipe from-channel to-channel))

        (if (and from-channel to-channel (not= :xform-provider from-channel-type) (= :xform-provider to-channel-type))
          (a/pipeline 1 to-channel (xform-fn (:etlp-config app-config)) from-channel))))
    @channels))


(defn traverse-dag [dag app-config]
  (let [entities (:entities dag)
        workflow (:workflow dag)
        channels (atom {})]
    (doseq [edge workflow]
      (let [[from-node to-node] edge
            from-channel (get @channels from-node)
            from-channel-type (:entity-type (get entities from-node))
            to-channel (get @channels to-node)
            to-channel-type (:entity-type (get entities to-node))
            from-fn (:processors (get entities from-node))
            xform-fn (:xform-provider (get entities to-node))]

        (if (and (nil? from-channel) (not= :xform-provider from-channel-type))
          (let [from-channel (from-fn (:etlp-config app-config))]
            (swap! channels assoc from-node from-channel)))

        (if (and (nil? to-channel) (not= :xform-provider to-channel-type))
          (let [to-channel (a/chan 1)]
            (swap! channels assoc to-node to-channel)))

        (if (and from-channel to-channel (not= :xform-provider from-channel-type) (not= :xform-provider to-channel-type))
          (a/pipe from-channel to-channel))

        (if (and from-channel to-channel (not= :xform-provider from-channel-type) (= :xform-provider to-channel-type))
(let [xform (xform-fn (:etlp-config app-config))]
(a/pipeline 1 to-channel xform from-channel)))))
@channels))
