(ns etlp.async
  (:require [clojure.core.async :as a]
            [clojure.tools.logging :refer [info warn]]
            [etlp.utils :refer [wrap-error wrap-log]])
  (:import [clojure.core.async.impl.channels ManyToManyChannel])
  (:gen-class))

(def rows (atom 0)) ; Dummy "database"

(defn save-into-database [batch]
  (swap! rows + (count batch))
  (info "Total Count of Records:: " @rows))


(def is-valid-json)
; parallel processing transducer
;TODO : should take a integrant based state of 
; log-channel 
; stdout-channel
; error-channel 
; record-channel 
; So that when we define processor we inject these channels to every processor app,
; chan should be injectable inside xform-provider or reducers to allow for logging 
; and idea is to allow etlp to stream out records, logs, error in json format on stdout
; the json format for now would be adhering to Airbyte protocol to wrap logs, errors and records
; this would allow end users to debug or transport pipeline execution logs out of standard platform specific loogers
(defn process-parallel [xf files]
  (let [ch (if (instance? ManyToManyChannel files) files (a/to-chan files))
        error-channel (a/chan 7) stdout (a/chan 7)]




    ;; (a/pipe (a/pipe error-channel stdout) (a/chan 7  (comp
    ;;                                                   (keep (fn [l] (println l) l))
    ;;                                                   (partition-all 10)
    ;;                                                   (keep save-into-database))))

    (a/<!!
     (a/pipeline
      6
      (a/pipe (a/pipeline 6
                          (doto (a/chan) (a/close!))
                          (comp
                          ;;  (map wrap-log)
                           (keep (fn [l] (println l) l))
                           (partition-all 10)
                           (keep save-into-database))
                          (a/pipe stdout error-channel)
                          true
                          (fn [error]
                            (a/go (a/>! error-channel (wrap-error {:error (str "Execption caught::" error)}))))) (a/pipe stdout error-channel))
      xf
      ch
      true
      (fn [error]
        (a/go (a/>! error-channel (wrap-error {:error (str "Execption caught::" error)}))))))

    (comment
      (a/go (while true
              (let [log (a/<! error-channel)]
                (warn log)))))))

(comment
  (defn process-with-transducers [transducer params files]
    (transduce
     (apply transducer params)
     (constantly nil)
     nil
     files)))
