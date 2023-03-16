(ns etlp.db
  (:require   [clojure.string :as str]
              [clojure.edn :as edn]
              [clojure.tools.logging :as log]
              [etlp.airbyte :refer [EtlpAirbyteSource]]
              [clj-postgresql.core :as pg]
              [etlp.connector :refer [connect]]
              [clojure.core.async :as async :refer [<! >! <!! >!! go-loop chan close! timeout]]
              [clojure.java.jdbc :as jdbc]
              [clojure.core.async :as a])
  (:import [clojure.core.async.impl.channels ManyToManyChannel])
  (:gen-class))


(defn create-connection [{:keys [host user dbname password port]}]
  (delay (pg/pool :host host :user user :dbname dbname :password password :port port)))

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
  (let [result-chan (async/chan)]
    (go-loop [offset (:offset-atom resource)]
      (let [page (.execute resource)]
        (async/>! result-chan (first page))
        (let [total (get-in (first page) [:json_build_object "total"])
              count (get-in (first page) [:json_build_object "count"])
              new-offset (+ @offset (.pageSize resource))]
          (println total count new-offset)
          (when (< new-offset total)
            (println ">>> should trigger poll")
            (Thread/sleep (.pollInterval resource))
            (reset! offset new-offset)
            (recur offset)))))
    result-chan))


(def create-jdbc-processor! (fn [opts]
                              (let [processor (map->JDBCResource opts)]
                                processor)))


(defn list-pg-processor [data]
  (let [opts (data :db-config)
        jdbc-reader (create-jdbc-processor! opts)]
    (println ">>>>invoked>>>> pg-processor " opts)
    (a/pipe (start jdbc-reader) (data :channel))))


(def get-pg-rows (fn [data]
                      (let [reducer (data :reducer)
                            output (a/chan (a/buffer 2000000) (mapcat reducer))]
                        output)))

(def etlp-processor (fn [ch]
                      (if (instance? ManyToManyChannel ch)
                        ch
                        (ch :channel))))



(defn pg-process-topology [{:keys [db-config processors reducers reducer]}]
  (let [entities {:list-pg-processor {:db-config db-config
                                      :channel   (a/chan (a/buffer (db-config :page-size)))
                                      :meta      {:entity-type :processor
                                                  :processor   (processors :list-pg-processor)}}
                  :read-pg-chunks    {:reducer (reducers reducer)
                                      :meta    {:entity-type :processor
                                                :processor   (processors :read-pg-chunks)}}
                  :etlp-output       {:channel (a/chan (a/buffer (db-config :page-size)))
                                      :meta    {:entity-type :processor
                                                :processor   (processors :etlp-processor)}}}
        workflow [[:list-pg-processor :read-pg-chunks]
                  [:read-pg-chunks :etlp-output]]]
    {:entities entities
     :workflow workflow}))


(defrecord EtlpAirbytePostgresSource [db-config processors topology-builder reducers reducer]
  EtlpAirbyteSource
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
          etlp         (connect topology)
          records      (atom 0)
          reducers     (get-in this [:reducers])
          xform        (get-in this [:reducer])
          data-channel (get-in etlp [:etlp-output :channel])]
     data-channel)))


(def create-postgres-source! (fn [{:keys [db-config reducers reducer] :as opts}]
                               (let [pg-connector (map->EtlpAirbytePostgresSource {:db-config        db-config
                                                                                   :processors       {:list-pg-processor list-pg-processor
                                                                                                      :read-pg-chunks    get-pg-rows
                                                                                                      :etlp-processor    etlp-processor}
                                                                                   :reducers         reducers
                                                                                   :reducer          reducer
                                                                                   :topology-builder pg-process-topology})]
                         pg-connector)))
