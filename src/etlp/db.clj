(ns etlp.db
  (:require   [clojure.string :as str]
              [clojure.tools.logging :as log]
              [clojure.core.async :as async :refer [<! >! <!! >!! go-loop chan close! timeout]]
              [org.postgresql.util :as pgutil]
              [clojure.java.jdbc :as jdbc]
              [java.util.concurrent :as javautil.concurrent])
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


(defn- get-total-count [table-name where-clause]
  (let [count-query (str "SELECT count(*) FROM " table-name where-clause)]
    (->> (jdbc/query conn count-query)
         (first)
         (get "count")
         (pgutil.parseInt))))

(defn- build-paginated-query [table-name where-clause page-size offset-atom]
  (str "SELECT json_build_object(
            'total', " (get-total-count table-name where-clause) ",
            'count', count(" table-name ".*)
                     FILTER (WHERE " table-name ".id > " @offset-atom ") ,
            'offset', " @offset-atom ",
            'results', json_agg(row_to_json(" table-name "))
          )
          FROM (" (str "SELECT * FROM " table-name where-clause " ORDER BY id LIMIT " page-size " OFFSET " @offset-atom ") " table-name ")")))

(defprotocol PaginatedResource
  (start [this interval-ms page-size query]))

(defrecord JdbcPaginatedResource [conn table-name where-clause]
  PaginatedResource
  (start [this interval-ms page-size query]
    (let [result-chan (chan)
          offset-atom (atom 0)
          query (build-paginated-query table-name where-clause page-size offset-atom)]
      (log/infof "Starting paginated query: %s" query)
      (go-loop []
        (let [start-time (System/currentTimeMillis)]
          (try
            (let [rows (jdbc/query conn query)]
              (doseq [row rows]
                (>! result-chan row))
              (let [total-count (->> rows
                                    first
                                    (get "total")
                                    (pgutil.parseInt))
                    count (count rows)]
                (log/infof "Fetched %d/%d rows, total count is %d" count page-size total-count)
                (when (< count page-size)
                  (log/info "All rows fetched, closing result channel and stopping query loop")
                  (close! result-chan)))
              (reset! offset-atom (+ @offset-atom page-size)))
            (catch Exception e
              (log/errorf "Error while fetching rows: %s" e)))
          (let [elapsed-ms (- (System/currentTimeMillis) start-time)
                wait-ms (max 0 (- interval-ms elapsed-ms))]
            (when (> wait-ms 0)
              (<! (timeout wait-ms))))
          (when-not (channel-closed? result-chan)
            (recur)))))))
