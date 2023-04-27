(ns etlp.processors.db
  (:require   [clojure.string :as str]
              [clojure.tools.logging :as log]
              [etlp.connector.protocols :refer [EtlpSource EtlpDestination]]
              [clj-postgresql.core :as pg]
              [etlp.connector.dag :as dag]
              [clojure.core.async :as a :refer [<! >! <!! >!! go-loop chan close! timeout]]
              [clojure.java.jdbc :as jdbc])
  (:import [clojure.core.async.impl.channels ManyToManyChannel])
  (:gen-class))


(defn create-connection [{:keys [host user dbname password port]}]
  (pg/pool :host host :user user :dbname dbname :password password :port port))

(defn close-connection [conn]
  (pg/close! conn))

(defn db-schema-migrated?
  "Check if the schema has been migrated to the database"
  [conn {:keys [table]}]
  (-> (jdbc/query conn
                  [(str "select count(*) from information_schema.tables "
                        (format "where table_name='%s'" (name table)))])
      first :count pos?))

(defn apply-schema-migration
  "Apply the schema to the database"
  [conn {:keys [table specs]}]
  (when (not (db-schema-migrated? conn {:table table}))
    (jdbc/db-do-commands conn
                         (jdbc/create-table-ddl
                          table
                          specs))))

(defn write-batch [conn table batch]
  (jdbc/insert-multi! conn table batch))

(defn pg-destination [db]
  (fn [table]
    (partial write-batch db table)))

(defn create-pg-connection [config]
  (create-connection config))

(defn create-pg-destination [db {:keys [table specs]}]
  (delay
   (apply-schema-migration db {:table table :specs specs})
   ((pg-destination db) table)))

(defprotocol PaginatedJDBCResource
  (execute [this])
  (total [this])
  (pageSize [this])
  (pollInterval [this]))

(defrecord JDBCResource [db-spec query page-size poll-interval offset-atom]
  PaginatedJDBCResource
  (execute [this]
    (let [page-size (:page-size this)
          offset-atom @(:offset-atom this)
          table-name (->> (re-find #"(?i)FROM (\w+)" query) second keyword)
          total (.total this)
          select-query (str "(" query " ORDER BY created_at LIMIT " page-size " OFFSET " offset-atom ")")
          sql (str "SELECT json_build_object(
                        'total', ", total, ",
                        'count', count(q.*),
                        'offset', ", offset-atom, ",
                        'results', json_agg(row_to_json(q))) FROM (" select-query ") AS q")
          results (jdbc/query db-spec sql)]
      (println "invoked")
      results))
  (total [this]
    (let [db-spec (:db-spec this)
          query (:query this)
          sql (str "SELECT COUNT(*) FROM (" query ") _")]
      (-> (jdbc/query db-spec sql)
          first
          (get :count))))
  (pageSize [this]
    (:page-size this))
  (pollInterval [this]
    (:poll-interval this)))


(defn start [resource]
  (let [result-chan (a/chan)]
    (go-loop [offset (:offset-atom resource)]
      (let [page (.execute resource)]
        (a/>! result-chan (first page))
        (let [total (get-in (first page) [:json_build_object "total"])
              count (get-in (first page) [:json_build_object "count"])
              new-offset (+ @offset (.pageSize resource))]
          (when (< new-offset total)
            (Thread/sleep (.pollInterval resource))
            (reset! offset new-offset)
            (recur offset)))))
    result-chan))


(def create-jdbc-processor! (fn [opts]
                              (let [processor (map->JDBCResource opts)]
                                processor)))


(defn list-pg-processor [data]
  (let [opts (data :db-config)
        jdbc-reader (create-jdbc-processor! opts)
        results (start jdbc-reader)]
    results))

(def get-pg-rows (fn [data]
                   (let [reducer (data :reducer)
                         output  (a/chan (a/buffer 2000000) (mapcat reducer))]
                        output)))

(def etlp-processor (fn [data]
                      (if (instance? ManyToManyChannel data)
                        data
                        (data :channel))))

(defn pg-process-topology [{:keys [db-config processors reducers reducer]}]
  (let [entities {:list-pg-processor {:db-config db-config
                                      :channel   (a/chan (a/buffer (db-config :page-size)))
                                      :meta      {:entity-type :processor
                                                  :processor   (processors :list-pg-processor)}}
                  :xform-processor {:meta {:entity-type :xform-provider
                                           :xform (reducers reducer)}}
                  :etlp-output {:channel (a/chan (a/buffer (db-config :page-size)))
                                :meta    {:entity-type :processor
                                          :processor   (processors :etlp-processor)}}}
        workflow [[:list-pg-processor :xform-processor]
                  [:xform-processor :etlp-output]]]
    {:entities entities
     :workflow workflow}))


(defrecord EtlpAirbytePostgresSource [db-config processors topology-builder reducers reducer]
  EtlpSource
  (spec [this] {:supported-destination-streams []
                :supported-source-streams      [{:stream_name "pg_stream"
                                                 :schema      {:type       "object"
                                                               :properties {:db-config  {:type        "object"
                                                                                         :description "S3 connection configuration."}

                                                                            :processors {:type        "object"
                                                                                         :description "Processors to be used to extract and transform data from the S3 bucket."}}}}]})

  (check [this]
    (let [errors (conj [] (when (nil? (:db-config this))
                            "s3-config is missing")
                       (when (nil? (:reducers this))
                         "bucket is missing")
                       (when (nil? (:processors this))
                         "processors is missing"))]
      {:status  (if (empty? errors) :valid :invalid)
       :message (if (empty? errors) "Source configuration is valid."
                    (str "Source configuration is invalid. Errors: " (clojure.string/join ", " errors)))}))

  (discover [this]
            ;; TODO use config and topology to discover schema from mappings
    {:streams [{:stream_name "pg_stream"
                :schema      {:type       "object"
                              :properties {:data {:type "string"}}}}]})
  (read! [this]
    (let [topology     (topology-builder this)
          workflow         (dag/build topology)]
          
     workflow)))


(def create-postgres-source! (fn [{:keys [db-config reducers reducer] :as opts}]
                               (let [pg-connector (map->EtlpAirbytePostgresSource {:db-config        db-config
                                                                                   :processors       {:list-pg-processor list-pg-processor
                                                                                                      :read-pg-chunks    get-pg-rows
                                                                                                      :etlp-processor    etlp-processor}
                                                                                   :reducers         reducers
                                                                                   :reducer          reducer
                                                                                   :topology-builder pg-process-topology})]
                                 pg-connector)))


(defn pg-destination-topology [{:keys [processors db]}]
  (let [db-conn    (create-connection (db :config))
        db-sink    (create-pg-destination db-conn db)
        threads    (db :threads)
        partitions (db :partitions)
        entities   {:etlp-input {:channel (a/chan (a/buffer partitions))
                                 :meta    {:entity-type :processor
                                           :processor   (processors :etlp-processor)}}

                    :etlp-output {:meta {:entity-type :xform-provider
                                         :threads     threads
                                         :partitions  partitions
                                         :xform       (comp
                                                       (partition-all partitions)
                                                       (map @db-sink)
                                                       (keep (fn [l] (println "Record created :: " (get (first l) :id)))))}}}
        workflow [[:etlp-input :etlp-output]]]

    {:entities entities
     :workflow workflow}))

(defrecord EtlpPostgresDestination [db processors topology-builder]
  EtlpDestination
  (write![this]
    (let [topology     (topology-builder this)
          etlp-inst         (dag/build topology)]
          
      etlp-inst)))

(def create-postgres-destination! (fn [{:keys [pg-config table specs threads partitions] :as opts}]
                                    (let [pg-destination (map->EtlpPostgresDestination {:db  {:config pg-config
                                                                                              :table table
                                                                                              :specs specs
                                                                                              :threads threads
                                                                                              :partitions partitions}
                                                                                        :processors
                                                                                        {:etlp-processor   etlp-processor}
                                                                                        :topology-builder pg-destination-topology})]
                                     pg-destination)))
