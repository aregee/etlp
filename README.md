# ETLP - Efficient Data Processing in Clojure

ETLP is a powerful Clojure library that simplifies the implementation of parallel concurrency and provides an efficient way to build data connectors. It leverages the concept of transducers and is based on the CSP (Communicating Sequential Processes) pattern. ETLP allows developers to easily incorporate best practices when working with data streams, with the intent of making it a go-to library for data processing in Clojure.

## Features
- Built on the concept of transducers and CSP pattern.
- Simplifies the implementation of parallel concurrency.
- Allows decoupling of data mapping logic (business logic) from code using etlp-mapper.
- Supports a wide range of source and destination processors.

## Example: Reading from S3 and Writing to Postgres or Stdout
The following example demonstrates how to create an ETLP connection to read from an S3 source and write to either a Postgres database or stdout stream:

```clojure
(ns myproject.hl7-processor
  (:require [clojure.string :as s]
            [etlp.core :as etlp]
            [etlp.stdout :refer [create-stdout-destination!]]
            [etlp.db :refer [create-postgres-destination!]]
            [etlp.s3 :refer [create-s3-source!]]
            [etlp.utils :refer [wrap-record wrap-log]]))


(defn create-hl7-processor [{:keys [config mapper]}]
  (let [s3-source {:s3-config (config :s3)
                   :bucket    (System/getenv "ETLP_TEST_BUCKET")
                   :prefix    "stormbreaker/hl7"
                   :reducers  {:hl7-reducer
                               (comp
                                (hl7-xform {})
                                (map (fn [segments]
                                       (s/join "\r" segments))))}
                   :reducer   :hl7-reducer}
        destination-conf {}]

    {:source (create-s3-source! s3-source)
     :destination (create-stdout-destination! destination-conf)
     :xform (comp (map wrap-record))
     :threads 16}))


(defn create-hl7-processor-postgres [{:keys [config mapper]}]
  (let [s3-source {:s3-config (config :s3)
                   :bucket    (System/getenv "ETLP_TEST_BUCKET")
                   :prefix    "stormbreaker/hl7"
                   :reducers  {:hl7-reducer
                               (comp
                                (hl7-xform {})
                                (map (fn [segments]
                                       (s/join "\r" segments))))}
                   :reducer   :hl7-reducer}
        destination-conf {:pg-config (config :db)
                          :table (table-opts :table)
                          :specs (table-opts :specs)}]

    {:source (create-s3-source! s3-source)
     :destination (create-postgres-destination! destination-conf)
     :xform (comp (map wrap-record))
     :threads 16}))

(def s3-config {:region "us-east-1"
                :credentials {:access-key-id (System/getenv "ACCESS_KEY_ID")
                              :secret-access-key (System/getenv "SECRET_ACCESS_KEY_ID")}})

(def db-config
  {:host (System/getenv "DB_HOSTNAME")
   :user (System/getenv "DB_USER")
   :dbname (System/getenv "DB_NAME")
   :password (System/getenv "DB_PASSWORD")
   :port 5432})

(def table-opts {:table :test_hl7_etlp
                 :specs  [[:id :serial "PRIMARY KEY"]
                          [:type :varchar]
                          [:version :varchar]
                          [:source_stream :varchar]
                          [:schema :varchar]
                          [:timestamp :timestamp]
                          [:data :varchar]
                          [:created_at :timestamp
                           "NOT NULL" "DEFAULT CURRENT_TIMESTAMP"]]})


(def hl7-processor {:name :airbyte-hl7-s3-connector
                    :process-fn  create-hl7-processor
                    :etlp-config {:s3 s3-config}
                    :etlp-mapper {:base-url "http://localhost:3000"
                                  :specs    {:adt-fhir       "13"
                                             :test-mapping "16"}}})

(def hl7-processor-postgres {:name :hl7-s3-connector-postgres
                    :process-fn  create-hl7-postgres-processor
                    :etlp-config {:s3 s3-config
                                  :db db-config}
                    :etlp-mapper {:base-url "http://localhost:3000"
                                  :specs    {:adt-fhir       "13"
                                             :test-mapping "16"}}})

(def airbyte-hl7-s3-connector {:id 1
                               :component :etlp.core/processors
                               :ctx hl7-processor})

(def hl7-s3-pg-connector {:id 2
                          :component :etlp.core/processors
                          :ctx hl7-processor-postgres})

(def etlp-app (etlp/init {:components [airbyte-hl7-s3-connector hl7-s3-pg-connector]}))

(etlp-app {:processor :airbyte-hl7-s3-connector :params {:command :etlp.core/start
                                                         :options {:foo :bar}}})

(etlp-app {:processor :airbyte-hl7-s3-connector-postgres :params {:command :etlp.core/start
                                                         :options {:foo :bar}}})


```

In this example, we define two processors: one for reading from an S3 source and writing to stdout, and another for reading from the S3 source and writing to a Postgres database. The etlp-app is initialized with both processors as components, and then each processor is started with the respective command.

## Getting Started
To get started with the ETLP library, follow the instructions in the official documentation.

## Contributing
We welcome contributions from the community! Please feel free to submit issues and pull requests on our GitHub [repository](https://github.com/aregee/etlp).

## License

Copyright 2023 Rahul Gaur

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
