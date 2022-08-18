(ns etlp.db
  (:require
   [clj-postgresql.core :as pg]
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