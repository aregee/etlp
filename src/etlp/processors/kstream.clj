(ns etlp.processors.kstream
  (:require [clojure.tools.logging :refer [debug info warn]]
            [integrant.core :as ig]
            [jackdaw.client :as jc]
            [jackdaw.streams :as js]
            [jackdaw.client.log :as jcl]
            [jackdaw.admin :as ja]
            [willa.core :as w]
            [etlp.utils.mapper :as em]
            [willa.viz :as wv]
            [jackdaw.serdes.edn :as serdes.edn])
  (:gen-class))

;; KSTREAM app
(defn start! [topology streams-config]
  (let [builder (doto (js/streams-builder)
                  (w/build-topology! topology))]
    (if (streams-config "etlp.topology.visualise")
      (do
        (wv/view-topology topology)
        (info "Visualizer Launched"))
      (info "Starting Streaming App" (streams-config "application.id")))
    (doto (js/kafka-streams builder
                            streams-config)
      (.setUncaughtExceptionHandler (reify Thread$UncaughtExceptionHandler
                                      (uncaughtException [_ t e]
                                        (warn e))))
      js/start)))


(defn- create-topics! [topic-metadata client-config]
  (let [admin (ja/->AdminClient client-config)]
    (doto (ja/create-topics! admin (vals topic-metadata))
      (.setUncaughtExceptionHandler (reify Thread$UncaughtExceptionHandler
                                      (uncaughtException [_ t e]
                                        (debug e)))))))

(defn exec-stream
  [config]
  (let [stream-app (ig/init config)]
    (get-in stream-app [::app :streams-app])))

(defn stream-conf
  "The production config.
  When the 'dev' alias is active, this config will not be used."
  [conf]
  {::config {:conf (conf :config)}

   ::options {:opts (conf :options)}

   ::mapper {:mapping-specs (:etlp-mapper conf)}

   ::topics {:client-config (select-keys (:kafka conf) ["bootstrap.servers"])
             :topic-metadata (:topic-metadata conf)}

   ::topology {:topology-builder (:process-fn conf)
               :mapper (ig/ref ::mapper)
               :config (ig/ref ::config)
               :options (ig/ref ::options)
               :topics (ig/ref ::topics)}

   ::app {:streams-config (conf :kafka)
          :topology (ig/ref ::topology)
          :topics (ig/ref ::topics)}})

(defn create-kstream-processor [{:keys [etlp-config etlp-mapper process-fn options] :as kstream-props}]

  (if-not (get-method ig/init-key ::topics)
  ;; Install this only if not already installed
    (defmethod ig/init-key ::topics [_ {:keys [client-config topic-metadata etlp-mapper]
                                        :as   opts}]
      (try
        (create-topics! topic-metadata client-config)
        (catch Exception e
          (debug (str "caught exception: " (.getMessage e)))))

      (assoc opts :topic-metadata topic-metadata)))

  (defmethod ig/init-key ::config [_ {:keys [conf]}] conf)

  (defmethod ig/init-key ::options [_ {:keys [opts]}] opts)

  (if-not (get-method ig/init-key ::mapper)
  ;; Install this only if not already installed
   (defmethod ig/init-key ::mapper
     [_ {:keys [mapping-specs] :as config}]
    (em/fetch-mappings mapping-specs)))

  (if-not (get-method ig/init-key ::topology)
  ;; Install this only if not already installed
    (defmethod ig/init-key ::topology [_ {:keys [topology-builder topics mapper config options] :as opts}]
      (assoc opts :topology (topology-builder {:config config
                                               :options options
                                               :topics (topics :topic-metadata)
                                               :mapper mapper}))))

  (if-not (get-method ig/init-key ::app)
  ;; Install this only if not already installed

    (defmethod ig/init-key ::app [_ {:keys [streams-config topology]
                                     :as   opts}]
      (assoc opts :streams-app (fn [ops] (start! (:topology topology) streams-config)))))

  (exec-stream (stream-conf {:kafka          (etlp-config :kafka)
                             :topic-metadata (etlp-config :topics)
                             :etlp-mapper    etlp-mapper
                             :options        options
                             :config         etlp-config
                             :process-fn     process-fn})))
