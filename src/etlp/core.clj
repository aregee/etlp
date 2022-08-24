(ns etlp.core
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info debug]]
            [etlp.db :as db]
            [etlp.reducers :as reducers]
            [integrant.core :as ig])
  (:gen-class))

(def *etl-config (atom nil))
(def *etlp-app (atom nil))

(def create-pg-connection db/create-pg-connection)

(def create-pg-destination db/create-pg-destination)

(def create-pipeline-processor reducers/parallel-directory-reducer)

(def json-reducer reducers/json-reducer)

(def file-reducer reducers/file-reducer)

(defn logger [line]
  (debug line)
  line)

(defn create-db-connection [config]
  @(create-pg-connection config))

(defn create-db-writer [db]
  (fn [opts]
    (create-pg-destination db opts)))

(defn create-json-stream [{:keys [db config table-opts xform-provider reducers sinks]}]
  (fn [opts]
    (let [directory-reducer (:directory-reducer reducers)
          sink ((:db-stream sinks) table-opts)
          jreduce ((:json-reducer reducers) opts)
          compose-xf (fn [params]
                       (comp (mapcat jreduce)
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

(defmulti etlp-component
  "Multi method to add extenstions to etlp"
  (fn [x]
    (get x :component)))

(defmethod etlp-component ::processors [{:keys [id component ctx]}]
  (let [plugin {:run (or (get ctx :process-fn) nil)
                :db (ig/ref ::db)
                :type (or (get ctx :type) nil)
                :config (ig/ref ::config)
                :table-opts (:table-opts ctx)
                :xform-provider (:xform-provider ctx)
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
                             ::sinks  {:db-stream {:sink create-db-writer
                                                   :db (ig/ref ::db)}}
                             ::default-processors {:json-processor create-json-stream}
                             ::processors {}}))))

(def schema (ig-wrap-schema {}))

(def json-reducer-def {:id 1
                       :component ::reducers
                       :ctx {:name :json-reducer
                             :xform-provider (fn [filepath opts]
                                               (map (reducers/parse-line filepath opts)))}})
(defn exec-processor
  "run etlp processor" [ctx {:keys [processor params]}]
  (let [executor (get-in ctx [:etlp.core/processors processor])]
    (executor params)))

(defn init [{:keys [components] :as params}]
  (schema)
  (etlp-component json-reducer-def)
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

  (defmethod ig/init-key ::sinks [_ {:keys [db-stream]
                                     :as opts}]
    (let [sink (:sink db-stream) db (:db db-stream) bound-sink (sink db)]
      (assoc opts :db-stream bound-sink)))

  (defmethod ig/init-key ::processors [_ processors]
    (reduce-kv (fn [acc k ctx]
                 (if (not (nil? (:type ctx)))
                   (assoc acc k ((get-in ctx [:default-processors (:type ctx)]) ctx))
                   (assoc acc k ((:run ctx) ctx)))) {} processors))

  (reset! *etlp-app (ig/init (schema)))
  (partial exec-processor @*etlp-app))
