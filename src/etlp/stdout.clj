(ns etlp.stdout
  (:require [clojure.core.async :as a]
            [clojure.pprint :refer [pprint]]
            [etlp.utils :refer [wrap-log]]
            [etlp.connector :refer [connect]]
            [etlp.airbyte :refer [EtlpAirbyteDestination]]
            [etlp.async :refer [save-into-database]])
  (:import [clojure.core.async.impl.channels ManyToManyChannel]))

(defn log-output [data]
  (while true
    (let [msg (a/<!! (data :channel))]
      (println msg))))

(def etlp-processor (fn [ch]
                      (if (instance? ManyToManyChannel ch)
                        ch
                        (ch :channel))))

(defn update-state! [rows batch]
  (swap! rows + (count batch))
  rows)
  
(defn log-state [state]
  (let [log (wrap-log (str "Total Count of Records:: " state))]
    (println log)
    log))

(defn stdout-topology [{:keys [processors connection-state]}]
  (let [records        (connection-state :records)
        partitions     (connection-state :partitions)
        threads        (connection-state :threads)
        count-records! (partial update-state! records)
        entities       {:etlp-input {:channel (a/chan (a/buffer partitions))
                                     :meta    {:entity-type :processor
                                               :processor   (processors :etlp-processor)}}

                        :etlp-output {
                                      :meta {:entity-type :xform-provider
                                             :threads     threads
                                             :partitions  partitions
                                             :xform       (comp
                                                       (map (fn [x] (println x)))
                                                       (partition-all  partitions)
                                                       (map count-records!)
                                                       (map deref)
                                                       (keep log-state))}}}
        workflow [[:etlp-input :etlp-output]]]

    {:entities entities
     :workflow workflow}))

(defrecord EtlpStdoutDestination [connection-state processors topology-builder]
  EtlpAirbyteDestination
  (write![this]
    (let [topology  (topology-builder this)
          etlp-inst (connect topology)]
      etlp-inst)))

(def create-stdout-destination! (fn [{:keys [s3-config bucket prefix reducers reducer threads partitions] :as opts}]
                                  (let [stdout-destination (map->EtlpStdoutDestination {:connection-state {:records (atom 0)
                                                                                                           :threads threads
                                                                                                           :partitions partitions}
                                                                                        :processors
                                                                                        {:etlp-processor   etlp-processor
                                                                                         :stdout-processor log-output}
                                                                                        :topology-builder stdout-topology})]
                                   stdout-destination)))
