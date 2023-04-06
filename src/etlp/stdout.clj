(ns etlp.stdout
  (:require [clojure.core.async :as a]
            [clojure.pprint :refer [pprint]]
            [etlp.utils :refer [wrap-log]]
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
  (println (wrap-log (str "Total Count of Records:: " @rows)))
  @rows)

(defn stdout-topology [{:keys [processors connection-state]}]
  (let [records (connection-state :records)
        count-records! (partial update-state! records)
        entities {:etlp-input {:channel (a/chan (a/buffer 100000))
                               :meta    {:entity-type :processor
                                         :processor   (processors :etlp-processor)}}

                  :etlp-output {:meta    {:entity-type :xform-provider
                                          :xform   (comp
                                                    (keep (fn [x] (println x) x))
                                                    (partition-all  100)
                                                    (keep count-records!))}}}
        workflow [[:etlp-input :etlp-output]]]

    {:entities entities
     :workflow workflow}))

(defrecord EtlpStdoutDestination [connection-state processors topology-builder]
  EtlpAirbyteDestination
  (write![this]
    (let [topology     (topology-builder this)
          etlp         (connect topology)
          data-channel (get-in etlp [:etlp-input :channel])]
      data-channel)))

(def create-stdout-destination! (fn [{:keys [s3-config bucket prefix reducers reducer] :as opts}]
                                  (let [stdout-destination (map->EtlpStdoutDestination {:connection-state {:records (atom 0)}
                                                                                        :processors
                                                                                        {:etlp-processor   etlp-processor
                                                                                         :stdout-processor log-output}
                                                                                        :topology-builder stdout-topology})]
                                   stdout-destination)))
