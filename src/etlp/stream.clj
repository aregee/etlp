(ns etlp.stream "Local Streaming App using async chan"
    (:require [clojure.tools.logging :refer [debug warn]]
              [etlp.db :refer [create-pg-connection create-pg-destination]]
              [integrant.core :as ig]
              [jackdaw.client :as jc]
              [jackdaw.serdes.edn :refer [serde]]
              [etlp.s3 :as es3]
              [clojure.pprint :refer [pprint]])
    (:gen-class))

(def serdes
  {:key-serde (serde)
   :value-serde (serde)})

(defn create-db-connection [config]
  (debug "create db conn" config)
  @(create-pg-connection config))

(defn create-db-writer [db]
  (fn [opts]
    (create-pg-destination db opts)))

(defn create-kafka-producer [{:keys [config]}]
  (delay (jc/producer config serdes)))

(defn build-message-topic [cfg]
  (merge cfg serdes))

(defn publish-to-kafka! [producer]
  (fn [topic [id payload]]
    (let [message-id id]
      (jc/produce! @producer topic message-id {:id message-id
                                               :value payload}))))

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

(defn- s3-pg-stream [{:keys [s3-config opts params table-opts xform-provider reducers sinks reducer]}]
  (let [bucket-reducer (:s3-bucket-reducer reducers)
        sink @((:pg-stream sinks) table-opts)
        s3c (es3/s3-invoke s3-config)
        merged-opts (merge opts {:s3-client s3c :s3-config s3-config})
        reducer-fn ((get reducers reducer) merged-opts)
        compose-xf (comp
                    ;; (mapcat (fn [resp] (resp :Contents)))
                    ;; (map (fn [contents] (contents :Key)))
                    (mapcat reducer-fn)
                    (xform-provider params)
                    (map sink))
        pg-reducer (bucket-reducer {:s3-client s3c :s3-config s3-config :pipeline compose-xf})]
    (pg-reducer opts)))


(defn- fs-pg-stream [{:keys [db opts params table-opts xform-provider reducers sinks reducer]}]
  (let [directory-reducer (:directory-reducer reducers)
        sink ((:pg-stream sinks) table-opts)
        reducer-fn ((get reducers reducer) opts)
        compose-xf (comp (mapcat reducer-fn)
                         (xform-provider params)
                         (map @sink))
        pg-reducer (directory-reducer {:pg-connection db :pipeline compose-xf})]
    (pg-reducer opts)))

(defn create-pg-stream [{:keys [source-type] :as args}]
  (if (= source-type :fs)
    (fs-pg-stream args)
    (if (= source-type :s3)
      (s3-pg-stream args)
      (warn "Unsupported Stream Processor type" source-type))))

(defn- s3-stdout-stream [{:keys [s3-config opts reducers reducer]}]
  (let [bucket-reducer (:stdout-s3-reducer reducers)
        s3c (es3/s3-invoke s3-config)
        merged-opts (merge opts {:s3-client s3c :s3-config s3-config})
        reducer-fn ((get reducers reducer) merged-opts)
        compose-xf (comp
                    (mapcat reducer-fn)
                    ;; (keep (fn [recrd] (println recrd)))
                    )
        processor (bucket-reducer {:s3-client s3c :s3-config s3-config :pipeline compose-xf})]
    (processor opts)))


(defn create-stdout-stream [{:keys [source-type] :as args}]
  (if (= source-type :s3)
    (s3-stdout-stream args)
    (if (= source-type :fs)
      (warn "TODO :: Implement fs stdout")
      (warn "Unsupported Stream Processor type" source-type))))

(defn- fs-kafka-stream [{:keys [topic xform-provider reducers sinks reducer params opts]}]
  (let [directory-reducer (:directory-reducer reducers)
        sink (partial (:kafka-stream sinks) topic)
        data-reducer ((get reducers reducer) opts)
        compose-xf (comp (mapcat data-reducer)
                         (xform-provider params)
                         (map sink)
                         (build-lagging-transducer (or (:throttle params) 500))
                         (map deref))]
    ((directory-reducer {:pg-connection nil :pipeline compose-xf}) opts)))

(defn- s3-kafka-stream [{:keys [s3-config topic xform-provider reducers sinks reducer params opts]}]
  (let [bucket-reducer (:s3-bucket-reducer reducers)
        sink (partial (:kafka-stream sinks) topic)
        s3c (es3/s3-invoke s3-config)
        merged-opts (merge opts {:s3-client s3c :s3-config s3-config})
        reducer-fn ((get reducers reducer) merged-opts)
        compose-xf (comp (mapcat reducer-fn)
                         (xform-provider params)
                         (map sink)
                         (build-lagging-transducer (or (:throttle params) 500))
                         (map deref))
        kafka-reducer (bucket-reducer {:s3-client s3c :s3-config s3-config :pipeline compose-xf})]
    (kafka-reducer opts)))

(defn create-kafka-stream [{:keys [source-type] :as args}]
  (if (= source-type :fs)
    (fs-kafka-stream args)
    (if (= source-type :s3)
      (s3-kafka-stream args)
      (warn "Unsupported Stream Processor type" source-type))))

(def build-pg-sink (fn [db-stream]
                     (let [pg-sink (:sink db-stream)
                           db (:db db-stream)
                           bound-pg-sink (pg-sink db)]
                       bound-pg-sink)))

(def build-kafka-producer (fn [kafka-stream]
                            (let [kafka-sink (:sink kafka-stream)
                                  create-producer (:producer kafka-stream)
                                  producer (create-producer kafka-stream)
                                  bound-kafka-sink (kafka-sink producer)]
                              bound-kafka-sink)))

(defn exec-cstream
  [config]
  (let [stream-app (ig/init config)]
    (get-in stream-app [:etlp.stream/app :stream-app])))

(defn- stream-conf
  "The production config.
  When the 'dev' alias is active, this config will not be used."
  [conf]
  {::db {:conn create-db-connection
         :config (:db conf)}

   ::reducers (:reducers conf)

   ::sinks {:pg-stream {:sink create-db-writer :db (ig/ref ::db) :config (:db conf)}
            :kafka-stream {:producer create-kafka-producer
                           :config (:kafka conf)
                           :sink publish-to-kafka!}}

   ::app {:streams-config conf
          :sinks (ig/ref ::sinks)
          :reducers (ig/ref ::reducers)}})

(defn- stdout-stream-conf
  "The production config.
  When the 'dev' alias is active, this config will not be used."
  [conf]
  {::reducers (:reducers conf)

   ::app {:streams-config conf
          :reducers (ig/ref ::reducers)}})


(defn directory-to-stdout-stream-processor [{:keys [config reducers reducer params source-type]}]

  (defmethod ig/init-key ::reducers [_ ctx]
    ctx)

  (defmethod ig/init-key ::app [_ {:keys [streams-config reducers]
                                   :as opts}]
    (assoc opts :stream-app (fn [args] (create-stdout-stream (merge {:opts args :reducers reducers} streams-config)))))

  (exec-cstream (stdout-stream-conf {:s3-config (config :s3)
                              :source-type source-type
                              :reducers reducers
                              :reducer reducer
                              :params params})))

(defn directory-to-kafka-stream-processor [{:keys [config reducers reducer topic xform-provider params source-type]}]

  (defmethod ig/init-key ::reducers [_ ctx]
    ctx)

  (defmethod ig/init-key ::sinks [_ {:keys [kafka-stream]
                                     :as opts}]
    (debug config)
    (assoc opts :kafka-stream (build-kafka-producer kafka-stream)))

  (defmethod ig/init-key ::db [_ ctx]
    ctx)

  (defmethod ig/init-key ::app [_ {:keys [streams-config sinks reducers]
                                   :as opts}]
    (assoc opts :stream-app (fn [args] (create-kafka-stream (merge {:opts args :sinks sinks :reducers reducers} streams-config)))))

  (exec-cstream (stream-conf {:kafka (config :kafka)
                              :s3-config (config :s3)
                              :source-type source-type
                              :reducers reducers
                              :reducer reducer
                              :topic topic
                              :xform-provider xform-provider
                              :params params})))


(defn directory-to-db-stream-processor [{:keys [config reducers reducer table-opts xform-provider params source-type]}]

  (defmethod ig/init-key ::db [_ {:keys [conn config]}]
    (let [db (conn config)]
      db))

  (defmethod ig/init-key ::reducers [_ ctx]
    ctx)

  (defmethod ig/init-key ::sinks [_ {:keys [pg-stream]
                                     :as opts}]
    (assoc opts :pg-stream (build-pg-sink pg-stream)))


  (defmethod ig/init-key ::app [_ {:keys [streams-config sinks reducers]
                                   :as opts}]
    (assoc opts :stream-app (fn [args] (create-pg-stream (merge {:opts args :sinks sinks :reducers reducers} streams-config)))))

  (exec-cstream (stream-conf {:db (config :db)
                              :s3-config (config :s3)
                              :source-type source-type
                              :reducers reducers
                              :reducer reducer
                              :table-opts table-opts
                              :xform-provider xform-provider
                              :params params})))