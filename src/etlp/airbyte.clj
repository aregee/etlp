(ns etlp.airbyte
  (:require [clojure.core.async :as async]
            [clojure.data.json :as json])
  (:gen-class))

(defn wrap-data [data type]
  (let [wrapped-data {:type type
                      :timestamp (System/currentTimeMillis)
                      :version "0.1.0"
                      :schema {"fields" [{"name" "field1" "type" "string"} {"name" "field2" "type" "integer"}] "primary_key" ["field1"]}
                      :source_stream {"name" "stream1"}
                      :data data}]
    (json/write-str wrapped-data)))

(defn wrap-record [data]
  (wrap-data data :record))

(defn wrap-error [data]
  (wrap-data data :error))

(defn wrap-log [data]
  (wrap-data data :log))

(defrecord ConnectorSpecification []
  etlp.airbyte/ConnectorSpecification
  (spec []
    {:name "my-connector"
     :config {:required [:host :port]
              :optional [:username :password]}}))

(defn check [config]
  (let [status (check-connection config)]
    (if (:success status)
      {:status :SUCCEEDED}
      {:status :FAILED :message (:error status)})))

(defn discover [config]
  (let [catalog (get-catalog config)]
    (if (:success catalog)
      {:status :SUCCEEDED :catalog (:data catalog)}
      {:status :FAILED :message (:error catalog)})))

(defn read [config catalog state]
  (let [input-channel (async/chan 1)
        output-channel (async/chan 1)
        error-channel (async/chan 1)
        transducer (comp (map airbyte/wrap-record))]
    (async/pipeline 1 output-channel transducer (read-data config catalog state) input-channel)
    (async/merge output-channel error-channel)))


(defrecord ConnectorSpecification []
  airbyte/ConnectorSpecification
  (spec []
    {:name "my-connector"
     :config {:required [:bucket :prefix]
              :optional [:aws-access-key :aws-secret-key]}}))

(defn wrap-data [data type]
  (let [wrapped-data {:type type
                      :timestamp (System/currentTimeMillis)
                      :version "0.1.0"
                      :schema {"fields" [{"name" "field1" "type" "string"} {"name" "field2" "type" "integer"}] "primary_key" ["field1"]}
                      :source_stream {"name" "stream1"}
                      :data data}]
    (json/write-str wrapped-data)))

(defn wrap-record [data]
  (wrap-data data :record))

(defn wrap-error [data]
  (wrap-data data :error))

(defn wrap-log [data]
  (wrap-data data :log))

(defn parse-protocol-file []
  (let [protocol-file (io/resource "airbyte_protocol.yml")
        protocol-string (slurp protocol-file)
        protocol-data (json/parse-string protocol-string)]
    (-> protocol-data :source :spec)))

(defrecord ConnectorSpecification []
  airbyte/ConnectorSpecification
  (spec []
    (parse-protocol-file)))


(defn parse-protocol-file []
  (let [protocol-file (io/resource "airbyte_protocol.yml")
        protocol-string (slurp protocol-file)
        protocol-data (json/parse-string protocol-string)]
    protocol-data))

(defrecord ConnectorSpecification []
  airbyte/ConnectorSpecification
  (spec []
    (let [protocol-data (parse-protocol-file)
          source-spec (-> protocol-data :source :spec)
          connection-spec (-> source-spec :connectionSpecification)
          auth-spec (-> source-spec :authSpecification)
          advanced-auth (-> source-spec :advanced_auth)
          supports-incremental (-> source-spec :supportsIncremental)
          supports-normalization (-> source-spec :supportsNormalization)
          supports-dbt (-> source-spec :supportsDBT)
          supported-destination-sync-modes (-> source-spec :supported_destination_sync_modes)
          documentation-url (-> source-spec :documentationUrl)
          changelog-url (-> source-spec :changelogUrl)]
      {:connectionSpecification connection-spec
       :authSpecification auth-spec
       :advancedAuth advanced-auth
       :supportsIncremental supports-incremental
       :supportsNormalization supports-normalization
       :supportsDBT supports-dbt
       :supportedDestinationSyncModes supported-destination-sync-modes
       :documentationUrl documentation-url
       :changelogUrl changelog-url})))


