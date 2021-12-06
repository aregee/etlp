(ns etlp.core
  (:require [clojure.string :as s]
            [clojure.core.async :as a]
            [cheshire.core :as json]
            [clj-postgresql.core :as pg]
            [clojure.java.jdbc :as jdbc]
            [clojure.java.io :as io])
  (:import [java.io BufferedReader])
  (:gen-class))


(defn files [dirpath]
  (into [] (.list (io/file dirpath))))

(defn with-path [dirpath file]
  (str dirpath file))

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
  (delay (create-connection config)))


(defn create-pg-destination [db {:keys [table specs ]}]
  (delay (apply-schenma-migration db {:table table :specs specs}))
  ((pg-destination db) table ))


(defn files-processor [dir]
  (map (partial with-path dir) (files dir)))

(defn lines-reducible [^BufferedReader rdr]
  (reify clojure.lang.IReduceInit
    (reduce [this f init]
      (try
        (loop [state init]
          (if (reduced? state)
            state
            (if-let [line (.readLine rdr)]
              (recur (f state line))
              state)))
        (finally (.close rdr))))))

(defn read-lines [file]
  (lines-reducible (io/reader file)))

(defn file-reducer [{:keys [record-generator operation]}]
  (fn [filepath]
    (prn filepath)
    (eduction
     (operation (record-generator filepath))
     (read-lines filepath))))

(defn parse-line [file]
  (fn [line]
    (json/decode line true)))

(def json-reducer
  (file-reducer {:record-generator parse-line :operation map}))


; parallel processing transducer
(defn process-parallel [transducer params files]
  (a/<!!
   (a/pipeline
    (.availableProcessors (Runtime/getRuntime)) ;; Parallelism factor
    (doto (a/chan) (a/close!))                  ;; Output channel - /dev/null
    (apply transducer params)
    (a/to-chan files))))

(defn process-with-transducers [transducer params files]
  (transduce
   (apply transducer params)
   (constantly nil)
   nil
   files))

(defn create-pipeline-processor [{:keys [pg-connection pipeline]}]
    (fn [{:keys [params path]}]
      (process-parallel pipeline [params] (files-processor path))
      (when (not= pg-connection :nil)
        (close-connection pg-connection)))
