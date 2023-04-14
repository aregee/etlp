(ns etlp.connector.protocols)
 

(defprotocol EtlpSource
  (spec [this] "Return the spec of the source.")
  (check [this] "Check the validity of the source configuration.")
  (discover [this] "Discover the available schemas of the source.")
  (read! [this] "Read data from the source and return a sequence of records."))

(defprotocol EtlpDestination
  (spec [this] "Return the spec of the source.")
  (check [this] "Check the validity of the source configuration.")
  (write! [this] "Read data from the source and return a sequence of records."))
