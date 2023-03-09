(ns etlp.connector
  (:require [clojure.core.async :as a]
            [clojure.pprint :refer [pprint]]
            [etlp.utils :refer [wrap-log]]
            [etlp.airbyte :refer [EtlpAirbyteDestination]]
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
      (let [output-channel (a/chan 1 xform)]
;        (a/pipeline 16 output-channel xform input-channel)
        (a/pipe input-channel output-channel))
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
  (start [this]
    (let [dest (:destination this)
          src  (:source this)
          xf   (:xform this)
          axfd (a/pipe src (a/chan 2000000000 xf))]
      (a/pipe axfd dest))))


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
        entities {:etlp-input {:channel (a/chan 2000000000)
                               :meta    {:entity-type :processor
                                         :processor   (processors :etlp-processor)}}

                  :etlp-output {
                                :meta    {:entity-type :xform-provider
                                          :xform   (comp
                                                    (keep (fn [x] (println x) x))
                                                    (partition-all  10)
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
                                                                                       {:etlp-processor    etlp-processor
                                                                                        :stdout-processor  log-output}
                                                                                       :topology-builder stdout-topology})]
                                  stdout-destination)))



(defn create-connection [{:keys [source destination xform] :as config}]
  (let [etlp-src     (etlp-source :read source)
        etlp-dest    (etlp-destination :write destination)
        connection (map->EtlpConnect {:config {:pf 1} :source etlp-src :destination etlp-dest :xform xform})]
    connection))
