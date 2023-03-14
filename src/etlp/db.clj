(ns etlp.db
  (:require   [clojure.string :as str]
              [clojure.edn :as edn]
              [clojure.tools.logging :as log]
              [honey.sql :as sql]
              [clj-postgresql.core :as pg]
              [clojure.core.async :as async :refer [<! >! <!! >!! go-loop chan close! timeout]]
              [clojure.java.jdbc :as jdbc])
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
  (execute-query [this query page-size offset-atom])
  (get-total [this query])
  (get-page-size [this query])
  (get-poll-interval [this]))

(defrecord JDBCResource [db-spec query page-size poll-interval]
  PaginatedJDBCResource
  (execute-query [this query page-size offset-atom]
    (let [table-name (->> (re-find #"(?i)FROM (\w+)" query) second keyword)
          total (.getTotal this query)
          select-query (str "SELECT total, count(*), json_agg(row_to_json(_))
                             FROM (" query ") AS _, LATERAL (SELECT COUNT(*) AS total FROM _ ) AS __
                             WHERE id > " offset-atom " GROUP BY total ORDER BY total LIMIT " page-size)
          sql (str "SELECT json_build_object(
                        'total', ", total, ",
                        'count', count,
                        'offset', ", offset-atom, ",
                        'results', ", "(SELECT json_agg(q) FROM (" select-query ") AS q)", "
                      )")
          results (jdbc/query db-spec sql)]
      results))
  (get-total [this query]
    (let [sql (str "SELECT COUNT(*) FROM (" query ") AS _")]
      (-> (jdbc/query db-spec sql)
          first
          (get :count))))
  (get-page-size [this query]
    (edn/read-string (str/replace-first query #"LIMIT (\d+).*" "$1")))
  (get-poll-interval [this]
    poll-interval))


(defn start [resource poll-interval]
  (let [result-chan (async/chan)]
    (go-loop [offset (atom 0)]
      (let [page (.executeQuery resource (.query resource) (.pageSize resource) @offset)]
        (async/>! result-chan page)
        (let [total (get-in page [0 "total"])
              count (get-in page [0 "count"])
              new-offset (+ @offset (.getPageSize resource (.query resource)))]
          (when (< new-offset total)
            (Thread/sleep (.getPollInterval resource))
            (reset! offset new-offset)
            (recur offset)))))
    result-chan))
