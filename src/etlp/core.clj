(ns etlp.core
  (:require   [etlp.reducers :as reducers]
              [etlp.db :as db])
  (:import [java.io BufferedReader])
  (:gen-class))


(def create-pg-connection db/create-pg-connection)

(def create-pg-destination db/create-pg-destination)

(def create-pipeline-processor reducers/parallel-directory-reducer)

(def json-reducer reducers/json-reducer)

(def file-reducer reducers/file-reducer)
