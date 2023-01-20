(ns etlp.async
  (:require   [clojure.core.async :as a])
  (:import [clojure.core.async.impl.channels ManyToManyChannel])
  (:gen-class))


; parallel processing transducer
(defn process-parallel [xf files]
  (let [ch (if (instance? ManyToManyChannel files) files (a/to-chan files))]
    (a/<!!
     (a/pipeline
      (dec (.availableProcessors (Runtime/getRuntime)));; Parallelism factor
      (doto (a/chan) (a/close!))                  ;; Output channel - /dev/null
      xf
      ch))))

(comment
  (defn process-with-transducers [transducer params files]
    (transduce
     (apply transducer params)
     (constantly nil)
     nil
     files)))
