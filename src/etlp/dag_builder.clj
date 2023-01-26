(ns etlp.dag-builder
  (:require [integrant.core :as ig]
            [clojure.core.async :as a]))

;; Define a record to hold the topology information
(defrecord DAGBuilder [topology])

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
(defn traverse-dag [dag]
  (let [entities (:entities dag)
        workflow (:workflow dag)
        channels (atom {})]
    (doseq [edge workflow]
      (let [[from-node to-node] edge
            from-channel (get @channels from-node)
            to-channel (get @channels to-node)
            from-fn (:processors (get entities from-node))
            xform-fn (:xform-provider (get entities to-node))]
        (if (nil? from-channel)
          (let [from-channel (from-fn (:s3-config app))]
            (swap! channels assoc from-node from-channel)))
        (if (nil? to-channel)
          (let [to-channel (a/chan 1)]
            (swap! channels assoc to-node to-channel)))
        (a/pipe (a/pipeline 1 to-channel xform-fn from-channel) (doto (a/chan) (a/close!)))))
    @channels))



