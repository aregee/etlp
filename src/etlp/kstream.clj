(ns etlp.kstream
  (:require [clojure.tools.logging :refer [debug info]]
            [integrant.core :as ig]
            [jackdaw.client :as jc]
            [jackdaw.streams :as js]
            [jackdaw.client.log :as jcl]
            [jackdaw.admin :as ja]
            [willa.core :as w]
            [willa.viz :as wv]
            [jackdaw.serdes.edn :refer [serde]]
            [clojure.core :as clj])
  (:gen-class))

;; KSTREAM app 


(defn start! [topology streams-config]
  (let [builder (doto (js/streams-builder)
                  (w/build-topology! topology))]
    (if (streams-config "etlp.topology.visualise")
      (wv/view-topology topology)
      (info "Starting Streaming App" (streams-config "application.id")))
    (doto (js/kafka-streams builder
                            streams-config)
      (.setUncaughtExceptionHandler (reify Thread$UncaughtExceptionHandler
                                      (uncaughtException [_ t e]
                                        (println e))))
      js/start)))


(defn create-topics! [topic-metadata client-config]
  (let [admin (ja/->AdminClient client-config)]
    (doto (ja/create-topics! admin (vals topic-metadata))
      (.setUncaughtExceptionHandler (reify Thread$UncaughtExceptionHandler
                                      (uncaughtException [_ t e]
                                        (debug e)))))))


(defn exec-stream
  [config]
  (let [stream-app (ig/init config)]
    (get-in stream-app [:etlp.kstream/app :streams-app])))

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

(defn kafka-stream-topology-processor [{:keys [config topic-metadata topic-reducers topology-builder params]}]

  (if-not (get-method ig/init-key ::topics)
  ;; Install this only if not already installed

    (defmethod ig/init-key ::topics [_ {:keys [client-config topic-metadata topic-reducers]
                                        :as opts}]

      (try
        (debug ">>>>INVOKING CREATE TOPIC <<<<<<<")
        (create-topics! topic-metadata client-config)
        (catch Exception e
          (debug (str "caught exception: " (.getMessage e)))))

      (assoc opts :topic-metadata topic-metadata :topic-reducers topic-reducers)))


  (if-not (get-method ig/init-key ::topology)
  ;; Install this only if not already installed

    (defmethod ig/init-key ::topology [_ {:keys [topology-builder topics] :as opts}]
      (assoc opts :topology (topology-builder (:topic-metadata topics) (:topic-reducers topics)))))


  (if-not (get-method ig/init-key ::app)
  ;; Install this only if not already installed

    (defmethod ig/init-key ::app [_ {:keys [streams-config topology]
                                     :as opts}]
      (assoc opts :streams-app (fn [] (start! (:topology topology) streams-config)))))

  (exec-stream (stream-conf {:kafka (:kafka config)
                             :topic-metadata topic-metadata
                             :topic-reducers topic-reducers
                             :topology-builder topology-builder})))
