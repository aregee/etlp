(ns etlp.async
  (:require   [clojure.core.async :as a])
  (:import [java.io BufferedReader])
  (:gen-class))


; parallel processing transducer
(defn process-parallel [xf params files]
  (a/<!!
   (a/pipeline
    (.availableProcessors (Runtime/getRuntime)) ;; Parallelism factor
    (doto (a/chan) (a/close!))                  ;; Output channel - /dev/null
    ;; (apply transducer params)
    xf
    (a/to-chan files))))

(comment
  (defn process-with-transducers [transducer params files]
    (transduce
     (apply transducer params)
     (constantly nil)
     nil
     files)))
