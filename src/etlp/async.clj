(ns etlp.async
  (:require [clojure.core.async :as a]
            [clojure.tools.logging :refer [warn]]
            [etlp.utils :refer [wrap-error]])
  (:import [clojure.core.async.impl.channels ManyToManyChannel])
  (:gen-class))

; parallel processing transducer
(defn process-parallel [xf files]
  (let [ch (if (instance? ManyToManyChannel files) files (a/to-chan files))
        error-channel (a/chan)]
    (a/<!!
     (a/pipeline
      (dec (.availableProcessors (Runtime/getRuntime)));; Parallelism factor
      (doto (a/chan) (a/close!))                  ;; Output channel - /dev/null
      xf
      ch
      true
      (fn [error]
        (a/go (a/>! error-channel (wrap-error {:error (str "Execption caught::" error)}))))))

    (a/go (while true
            (let [log (a/<! error-channel)]
              (warn log))))))

(comment
  (defn process-with-transducers [transducer params files]
    (transduce
     (apply transducer params)
     (constantly nil)
     nil
     files)))
