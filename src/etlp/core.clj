(ns etlp.core
  (:require [clojure.tools.logging :refer [debug info]]
            [etlp.db :as db]
            [etlp.reducers :as reducers]
            [integrant.core :as ig]
            [jackdaw.client :as jc]
            [jackdaw.streams :as js]
            [jackdaw.client.log :as jcl]
            [jackdaw.admin :as ja]
            [willa.core :as w]
            [jackdaw.serdes.edn :refer [serde]])
  (:gen-class))

(def *etl-config (atom nil))
(def *etlp-app (atom nil))

(def create-pg-connection db/create-pg-connection)

(def create-pg-destination db/create-pg-destination)

(def create-pipeline-processor reducers/parallel-directory-reducer)

(def json-reducer reducers/json-reducer)

(def file-reducer reducers/file-reducer)



;; KSTREAM app 


(defn start! [topology streams-config]
  (let [builder (doto (js/streams-builder)
                  (w/build-topology! topology))]
    ;; (wv/view-topology topology)
    (doto (js/kafka-streams builder
                            streams-config)
      (.setUncaughtExceptionHandler (reify Thread$UncaughtExceptionHandler
                                      (uncaughtException [_ t e]
                                        (println e))))
      js/start)))


(defn create-topics! [topic-metadata client-config]
  (let [admin (ja/->AdminClient client-config)]
    (clojure.pprint/pprint (vals topic-metadata))
    (doto (ja/create-topics! admin (vals topic-metadata))
      (.setUncaughtExceptionHandler (reify Thread$UncaughtExceptionHandler
                                      (uncaughtException [_ t e]
                                        (info e)))))))


(defn exec-stream
  [config]
  (let [stream-app (ig/init config)]
    (get-in stream-app [:etlp.core/app :streams-app])))

(defn stream-conf
  "The production config.
  When the 'dev' alias is active, this config will not be used."
  [conf]
  {::topics {:client-config (select-keys (:kafka conf) ["bootstrap.servers"])
             :topic-metadata (:topic-metadata conf)
             :topic-reducers (:topic-reducers conf)}


   ::topology {:topology-builder (:topology-builder conf)
               :topics (ig/ref ::topics)}

   ::app {:streams-config (:kafka conf)
          :topology (ig/ref ::topology)
          :topics (ig/ref ::topics)}})

(defn create-etlp-stream-processor [{:keys [config topic-metadata topic-reducers topology-builder params]}]


  (if-not (get-method ig/init-key ::topics)
  ;; Install this only if not already installed

    (defmethod ig/init-key ::topics [_ {:keys [client-config topic-metadata topic-reducers]
                                        :as opts}]

      (try
        (prn (vals topic-metadata))
        (create-topics! topic-metadata client-config)
        (catch Exception e (str "caught exception: " (.getMessage e))))

      (assoc opts :topic-metadata topic-metadata :topic-reducers topic-reducers)))


  (if-not (get-method ig/init-key ::topology)
  ;; Install this only if not already installed

    (defmethod ig/init-key ::topology [_ {:keys [topology-builder topics] :as opts}]
      (assoc opts :topology (topology-builder (:topic-metadata topics) (:topic-reducers topics)))))


  (if-not (get-method ig/init-key ::app)
  ;; Install this only if not already installed

    (defmethod ig/init-key ::app [_ {:keys [streams-config topology]
                                     :as opts}]
    ;; (wv/view-topology topology)
      (assoc opts :streams-app (fn [] (start! (:topology topology) streams-config)))))

  (exec-stream (stream-conf {:kafka (:kafka config)
                             :topic-metadata topic-metadata
                             :topic-reducers topic-reducers
                             :topology-builder topology-builder})))


(defn logger [line]
  (debug line)
  line)

(defn create-db-connection [config]
  @(create-pg-connection config))

(defn create-db-writer [db]
  (fn [opts]
    (create-pg-destination db opts)))

(defn create-pg-stream [{:keys [db config table-opts xform-provider reducers sinks reducer]}]
  (fn [opts]
    (let [directory-reducer (:directory-reducer reducers)
          sink ((:db-stream sinks) table-opts)
          reducer-fn ((get reducers reducer) opts)
          compose-xf (fn [params]
                       (comp (mapcat reducer-fn)
                             (xform-provider params)
                             (map @sink)))]
      (directory-reducer {:pg-connection db :pipeline compose-xf}))))

(defn custom-file-reducer [xform-provider]
  (fn [opts]
    (fn [filepath]
      (info filepath)
      (eduction
       (xform-provider filepath opts)
       (reducers/read-lines filepath)))))

(def serdes
  {:key-serde (serde)
   :value-serde (serde)})


(defn create-kafka-producer [{:keys [config]}]
  (delay (jc/producer config serdes)))

(defn build-message-topic [cfg]
  (merge cfg serdes))

(defn publish-to-kafka! [producer]
  "Publish a message to the provided topic"
  (fn [topic [id payload]]
    (let [message-id id]
      @(jc/produce! @producer topic message-id {:id message-id
                                                :value payload}))))

(defn create-kafka-stream [{:keys [topic xform-provider reducers sinks reducer]}]
  (fn [opts]
    ;; (prn opts reducer)
    (let [directory-reducer (:directory-reducer reducers)
          sink (partial (:kafka-stream sinks) topic)
          data-reducer ((get reducers reducer) opts)
          compose-xf (fn [params]
                       (comp (mapcat data-reducer)
                             (xform-provider params)
                             (map sink)))]
      ;; (pprint sinks)
      (directory-reducer {:pg-connection nil :pipeline compose-xf}))))

(defn create-kstream-processor [{:keys [config topic-metadata topology-builder topic-reducers] :as ctx}]
  (fn [opts]
    (create-etlp-stream-processor (merge ctx {:params opts}))))

(defmulti etlp-component
  "Multi method to add extenstions to etlp"
  (fn [x]
    (get x :component)))

(defmethod etlp-component ::processors [{:keys [id component ctx]}]
  (let [plugin {:run (or (get ctx :process-fn) nil)
                :db (ig/ref ::db)
                :type (or (get ctx :type) nil)
                :config (ig/ref ::config)
                :table-opts (or (get ctx :table-opts) nil)
                :topic (or (get ctx :topic) nil)
                :topic-metadata (or (get ctx :topic-metadata) nil)
                :topic-reducers (or (get ctx :topology-reducers) nil)
                :topology-builder (or (get ctx :topology-builder) nil)
                :reducer (or (get ctx :reducer) nil)
                :xform-provider (or (get ctx :xform-provider) nil)
                :reducers (ig/ref ::reducers)
                :sinks (ig/ref ::sinks)
                :default-processors (ig/ref ::default-processors)}]
    (swap! *etl-config assoc-in [::processors (:name ctx)] plugin)))

(defmethod etlp-component ::reducers [{:keys [id component ctx]}]
  (let [plugin (custom-file-reducer (:xform-provider ctx))]
    (swap! *etl-config assoc-in [::reducers (:name ctx)] plugin)))

(defmethod etlp-component ::config [{:keys [id component ctx]}]
  (let [plugin ctx]
    (swap! *etl-config assoc-in [::config (:name plugin)] (dissoc plugin :name))))

(defmethod etlp-component :default [params]
  (throw (IllegalArgumentException.
          (str "I don't know the " (get params :component) " support"))))

(defn ig-wrap-schema [params]
  (fn []
    (if-let [shape @*etl-config]
      shape
      (reset! *etl-config   {::config {:db nil
                                       :kafka nil}
                             ::db {:conn create-db-connection
                                   :config (ig/ref ::config)}
                             ::reducers {:directory-reducer create-pipeline-processor
                                         :file-reducer file-reducer}
                             ::sinks  {:config (ig/ref ::config)
                                       :db-stream {:sink create-db-writer
                                                   :db (ig/ref ::db)}
                                       :kafka-stream {:producer create-kafka-producer
                                                      :config (ig/ref ::config)
                                                      :sink publish-to-kafka!}}
                             ::default-processors {:pg-json-processor {:run create-pg-stream :reducer :json-reducer}}
                             ::processors {}}))))

(def schema (ig-wrap-schema {}))

(def json-reducer-def {:id 1
                       :component ::reducers
                       :ctx {:name :json-reducer
                             :xform-provider (fn [filepath opts]
                                               (map (reducers/parse-line filepath opts)))}})
(def line-reducer-def {:id 1
                       :component ::reducers
                       :ctx {:name :line-reducer
                             :xform-provider (fn [filepath opts]
                                               (map (fn [l] l)))}})

(defn exec-processor
  "run etlp processor" [ctx {:keys [processor params]}]
  (let [executor (get-in ctx [:etlp.core/processors processor])]
    (executor params)))


(def build-pg-sink (fn [db-stream]
                     (let [pg-sink (:sink db-stream)
                           db (:db db-stream)
                           bound-pg-sink (pg-sink db)]
                       bound-pg-sink)))

(def build-kafka-producer (fn [config kafka-stream]
                            (let [kafka-sink (:sink kafka-stream)
                                  create-producer (:producer kafka-stream)
                                  producer (create-producer {:config (:kafka config)})
                                  bound-kafka-sink (kafka-sink producer)]
                              bound-kafka-sink)))



(defn init [{:keys [components] :as params}]
  (schema)
  (etlp-component json-reducer-def)
  (etlp-component line-reducer-def)
  (loop [x (dec (count components))]
    (when (>= x 0)
      (etlp-component (nth components x))
      (recur (dec x))))


  (defmethod ig/init-key ::config [_ {:keys [db kafka] :as opts}] opts)

  (defmethod ig/init-key ::db [_ {:keys [conn config]
                                  :as opts}]
    (let [db (conn (:db config))]
      db))

  (defmethod ig/init-key ::reducers [_ ctx]
    ctx)

  (defmethod ig/init-key ::default-processors [_ ctx]
    ctx)

  (defmethod ig/init-key ::sinks [_ {:keys [db-stream kafka-stream config]
                                     :as opts}]

    (assoc opts :db-stream (build-pg-sink db-stream) :kafka-stream (build-kafka-producer config kafka-stream)))


  (defmethod ig/init-key ::processors [_ processors]
    (reduce-kv (fn [acc k ctx]
                 (if (not (nil? (:type ctx)))
                   (let [process-fn (get-in ctx [:default-processors (:type ctx) :run])
                         reducer (get-in ctx [:default-processors (:type ctx) :reducer])]
                     (assoc acc k (process-fn (merge ctx {:reducer reducer}))))
                   (assoc acc k ((:run ctx) ctx)))) {} processors))

  (reset! *etlp-app (ig/init (schema)))
  (partial exec-processor @*etlp-app))
