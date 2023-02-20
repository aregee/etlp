(ns etlp.airbyte
  (:require [clojure.core.async :as a :refer [<!!]]
            [etlp.connector :as connector]))


(defprotocol EtlpAirbyteSource
  (spec [this] "Return the spec of the source.")
  (check [this] "Check the validity of the source configuration.")
  (discover [this] "Discover the available schemas of the source.")
  (read! [this] "Read data from the source and return a sequence of records."))


