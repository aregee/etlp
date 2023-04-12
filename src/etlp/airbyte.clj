(ns etlp.airbyte)
 

(defprotocol EtlpAirbyteSource
  (spec [this] "Return the spec of the source.")
  (check [this] "Check the validity of the source configuration.")
  (discover [this] "Discover the available schemas of the source.")
  (read! [this] "Read data from the source and return a sequence of records."))

(defprotocol EtlpAirbyteDestination
  (spec [this] "Return the spec of the source.")
  (check [this] "Check the validity of the source configuration.")
  (write! [this] "Read data from the source and return a sequence of records."))


(comment
  (defrecord ConnectoSpecification)
  (defrecord AirbyteConnectionStatus)

 (defrecord
     AirbyteCatalog
     [])

 (defrecord
     ConfiguredAirbyteCatalog
     [])

 (defrecord
     AirbyteRecordMessage
     [])

 (defrecord
      AirbyteStateMessage
      []))
