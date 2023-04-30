(ns etlp.processors.kafka
  (:require [clojure.tools.logging :refer [debug warn]]
            [clojure.core.async :as a :refer [<! >! <!! >!! go-loop chan close! timeout]]
            [jackdaw.client :as jc]
            [etlp.connector.protocols :refer [EtlpSource EtlpDestination]]
            [etlp.connector.dag :as dag]
            [jackdaw.serdes.edn :refer [serde]]
            [jackdaw.admin :as ja])
  (:import [clojure.core.async.impl.channels ManyToManyChannel])
  (:gen-class))

(def serdes
  {:key-serde (serde)
   :value-serde (serde)})

(defn create-kafka-producer [{:keys [kafka]}]
  (jc/producer kafka serdes))

(defn publish-to-kafka! [producer topic]
  "Meththod should enforce record type as ETLP Record"
  (fn [[id record]]
    (jc/produce! producer topic id record)))

(defn build-lagging-transducer
  "creates a transducer that will always run n items behind.
   this is convenient if the pipeline contains futures, which you
   want to start deref-ing only when a certain number are in flight"
  [n]
  (fn [rf]
    (let [qv (volatile! clojure.lang.PersistentQueue/EMPTY)]
      (fn
        ([] (rf))
        ([acc] (reduce rf acc @qv))
        ([acc v]
         (vswap! qv conj v)
         (if (< (count @qv) n)
           acc
           (let [h (peek @qv)]
             (vswap! qv pop)
             (rf acc h))))))))

(defn- create-topics! [topic-metadata client-config]
  (let [admin (ja/->AdminClient client-config)]
    (doto (ja/create-topics! admin (vals topic-metadata))
      (.setUncaughtExceptionHandler (reify Thread$UncaughtExceptionHandler
                                      (uncaughtException [_ t e]
                                        (debug e)))))))
(def etlp-processor (fn [data]
                      (if (instance? ManyToManyChannel data)
                        data
                        (data :channel))))

(defrecord EtlpKafkaDestination [config processors topology-builder]
  EtlpDestination
  (write! [this]
    (let [topology (topology-builder this)
          etlp-dag (dag/build topology)]
      etlp-dag)))

(defn kafka-destination-topology [{:keys [config processors]}]
  (let [kafka-producer (create-kafka-producer config)
        topic          (get-in config [:topics :etlp-input])
        kafka-sink     (publish-to-kafka! kafka-producer topic)
        threads        (config :threads)
        partitions     (config :partitions)
        ;; topics         (create-topics! (config :topics) (select-keys (config :kafka) ["bootstrap.servers"]))
        entities       {:etlp-input  {:channel (a/chan (a/buffer partitions))
                                      :meta    {:entity-type :processor
                                                :processor   (processors :etlp-processor)}}
                        :etlp-output {:meta {:entity-type :xform-provider
                                             :threads     threads
                                             :partitions  partitions
                                             :xform       (comp
                                                           (map kafka-sink)
                                                           (build-lagging-transducer partitions)
                                                           (map deref)
                                                           (keep (fn [l] (println "Record created :: "  l) )))}}}
        workflow       [[:etlp-input :etlp-output]]]
    {:entities entities
     :workflow workflow}))

(def create-kafka-destination! (fn [{:keys [etlp-config threads partitions xform] :as opts}]
                                 (println etlp-config)
                                (let [kafka-dest (map->EtlpKafkaDestination {:config (merge {:threads threads
                                                                                      :partitions partitions} etlp-config)
                                                                             :processors {:etlp-processor etlp-processor}
                                                                             :topology-builder kafka-destination-topology})]
                                  kafka-dest)))
