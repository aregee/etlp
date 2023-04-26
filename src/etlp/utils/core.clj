(ns etlp.utils.core
  (:require [cheshire.core :as json]))

(defn wrap-data [data type]
  (let [wrapped-data {:type type
                      :timestamp (System/currentTimeMillis)
                      :version "0.1.0"
                      :schema "etlp_raw" ;{"fields" [{"name" "field1" "type" "string"} {"name" "field2" "type" "integer"}] "primary_key" ["field1"]}
                      :source_stream "etlp-stream"
                      :data data}]
    wrapped-data))

(defn wrap-record [data]
  (wrap-data data :record))

(defn wrap-error [data]
  (wrap-data data :error))

(defn wrap-log [data]
  (wrap-data data :log))
