(ns etlp.core
  (:require   [etlp.reducers :as reducers]
              [etlp.db :as db]
              [integrant.core :as ig])
  (:import [java.io BufferedReader])
  (:gen-class))

(def *etl-config (atom nil))

(def create-pg-connection db/create-pg-connection)

(def create-pg-destination db/create-pg-destination)

(def create-pipeline-processor reducers/parallel-directory-reducer)

(def json-reducer reducers/json-reducer)

(def file-reducer reducers/file-reducer)


(defn create-db-connection [config]
  (prn config)
  (create-pg-connection config))

(defn create-db-writer [db]
  (fn [opts]
    (prn db opts)
    (create-pg-destination db opts)))


(defn create-json-stream [{:keys [db config table-opts xform-provider reducers sinks]}]
  (fn [opts]
    (let [directory-reducer (:directory-reducer reducers)
          sink ((:db-stream sinks) table-opts)
          compose-xf (fn [params]
                       (comp (mapcat ((:json-reducer reducers) opts))   ;; Pipeline transducer
                             (xform-provider params)
                             (map @sink)))]
      (directory-reducer {:pg-connection (:conn db) :pipeline compose-xf}))))


(defn custom-file-reducer [xform-provider]
  (fn [opts]
    (prn ">>>ivoked>>>>" opts)
    (fn [filepath]
      (prn filepath)
      (eduction
       (prn (xform-provider filepath opts))
       (reducers/read-lines filepath)))))

(defmulti etlp-component
  "Multi method to add extenstions to etlp"
  (fn [x]
    (get x :component)))

(defmethod etlp-component ::processors [{:keys [id component ctx]}]
  (let [plugin {:run (:process-fn ctx)
                :db (ig/ref ::db)
                :config (ig/ref ::config)
                :table-opts (:table-opts ctx)
                :xform-provider (:xform-provider ctx)
                :reducers (ig/ref ::reducers)
                :sinks (ig/ref ::sinks)}]
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
                             ::processors {}}))))

(def schema (ig-wrap-schema {}))


(def json-reducer-def {:id 1
                       :component ::reducers
                       :ctx {:name :json-reducer
                             :xform-provider (fn [filepath opts]
                                               (comp (map reducers/parse-line)))}})

(def table-opts {:table :test_log_clj
                 :specs  [[:id :serial "PRIMARY KEY"]
                          [:type :varchar]
                          [:field :varchar]
                          [:created_at :timestamp
                           "NOT NULL" "DEFAULT CURRENT_TIMESTAMP"]]})

(def json-processor-def {:id 1
                         :component ::processors
                         :ctx {:name :json-processor
                               :process-fn create-json-stream
                               :table-opts table-opts
                               :xform-provider (fn [ag] (comp (map prn)))}})


(defn init [{:keys [components] :as params}]
  (schema)
  (etlp-component json-reducer-def)
  (etlp-component json-processor-def)
  ;; (prn (count components))
  (loop [x (dec (count components))]
    (when (>= x 0)
      (etlp-component (nth components x))
      (recur (dec x))))

  (if-not (get-method ig/init-key ::config)
    (defmethod ig/init-key ::config [_ {:keys [db kafka] :as opts}] opts))

  (if-not (get-method ig/init-key ::db)
  ;; Install this only if not already installed
    (defmethod ig/init-key ::db [_ {:keys [conn config]
                                    :as opts}]
      (let [db (conn (:db config))]
        (prn @db)
        @db)))

  (if-not (get-method ig/init-key ::reducers)
  ;; Install this only if not already installed

    (defmethod ig/init-key ::reducers [_ ctx]
      ctx))

  (if-not (get-method ig/init-key ::sinks)
  ;; Install this only if not already installed

    (defmethod ig/init-key ::sinks [_ {:keys [db-stream db]
                                       :as opts}]
    ;; (info topology)
      (let [sink (:sink db-stream)]
        (prn db)
        (assoc opts :db-stream (sink (:conn db)))
        )))

  (if-not (get-method ig/init-key ::processors)
    (defmethod ig/init-key ::processors [_ processors]
      (reduce-kv (fn [acc k ctx]
                   (assoc acc k ((:run ctx) (dissoc ctx :run)))) {} processors)))

  (ig/init @*etl-config))

