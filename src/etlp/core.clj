(ns etlp.core
  (:require   [etlp.reducers :as reducers]
              [etlp.db :as db]
              [integrant.core :as ig])
  (:import [java.io BufferedReader])
  (:gen-class))


(def create-pg-connection db/create-pg-connection)

(def create-pg-destination db/create-pg-destination)

(def create-pipeline-processor reducers/parallel-directory-reducer)

(def json-reducer reducers/json-reducer)

(def file-reducer reducers/file-reducer)

(defn create-db-connection [config]
  @(create-pg-connection config))

(defn create-db-writer [db]
  (fn [opts]
    (create-pg-destination (:conn db) (:table-opts opts))))


(defn create-json-stream [db config table-opts xform-provider reducers streams]
  (fn [opts]
    (let [directory-reducer (:directory-reducer reducers)
          sink ((get-in streams [:db-stream :sink]) table-opts)
          compose-xf (fn [params]
                       (comp (mapcat (:json-reducer reducers))   ;; Pipeline transducer
                             (xform-provider params)
                             (map @sink)))]
      (directory-reducer {:pg-connection (:conn db) :pipeline compose-xf}))))


(defn custom-file-reducer [xform-provider]
  (fn [opts]
    (fn [filepath]
      (prn filepath)
      (eduction
       (xform-provider filepath opts) ; should return xform
       (reducers/read-lines filepath)))))


(def *etl-config (atom nil))


(defn ig-wrap-schema [params]
  (fn []
    (if-let [shape @*etl-config]
      shape
      (reset! *etl-config   {::config {:db {}
                                       :kafka {}}
                             ::db {:conn create-db-connection
                                   :config (ig/ref ::config)}
                             ::reducers {:directory-reducer create-pipeline-processor
                                         :file-reducer file-reducer}

                             ::streams  {:db-stream {:sink create-db-writer
                                                     :db (ig/ref ::db)}}

                             ::pipelines {}}))))

(def schema (ig-wrap-schema {}))

(schema)

(defmulti etlp-component
  "Multi method to add extenstions to etlp"
  (fn [x]
    (get x :component)))

(defmethod etlp-component ::pipelines [{:keys [id component ctx]}]
  (let [plugin {:run (:process-fn ctx)
                :db (ig/ref ::db)
                :config (ig/ref ::config)
                :table-opts (:table-opts ctx)
                :xform-provider (:xform-provider ctx)
                :reducers (ig/ref ::reducers)
                :streams (ig/ref ::streams)}]
    (swap! *etl-config assoc-in [::pipelines (:name ctx)] plugin)))

(defmethod etlp-component ::reducers [{:keys [id component ctx]}]
  (let [plugin (custom-file-reducer (:xform-provider ctx))]
    (swap! *etl-config assoc-in [::pipelines (:name ctx)] plugin)))

(defmethod etlp-component :default [params]
  (throw (IllegalArgumentException.
          (str "I don't know the " (get params :component) " support"))))

(def json-reducer-def {:id 1
                       :component ::reducers
                       :ctx {:name :json-reducer
                             :xform-provider (fn [filepath opts]
                                               (comp (map reducers/parse-line)))}})

(def json-processor-def {:id 1
                         :component ::pipelines
                         :ctx {:name :json-processor
                               :process-fn create-json-stream
                               :table-opts {}
                               :xform-provider (fn [ag] (comp (map prn)))}})
