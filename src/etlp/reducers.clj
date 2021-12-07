(ns etlp.reducers
  (:require   [etlp.db :as db]
              [etlp.async :as async]
              [cheshire.core :as json]
              [clojure.java.io :as io])
  (:import [java.io BufferedReader])
  (:gen-class))

(defn files [dirpath]
  (into [] (.list (io/file dirpath))))

(defn with-path [dirpath file]
  (str dirpath file))
(defn files-processor [dir]
  (map (partial with-path dir) (files dir)))

; Wrapper to do readline on file in a transducer compatible reducible entity
(defn lines-reducible [^BufferedReader rdr]
  (reify clojure.lang.IReduceInit
    (reduce [this f init]
      (try
        (loop [state init]
          (if (reduced? state)
            state
            (if-let [line (.readLine rdr)]
              (recur (f state line))
              state)))
        (finally (.close rdr))))))

(defn read-lines [file]
; Using this method would allow you to 
; treat each line as a transducible entity which can allow you to apply various 
; other operations that you want to apply to reach row  eg filter, transform, map, etc
  (lines-reducible (io/reader file)))

(defn file-reducer [{:keys [record-generator operation]}]
  (fn [filepath]
    (prn filepath)
    (eduction
     (operation (record-generator filepath))
     (read-lines filepath))))

(defn parse-line [file]
  (fn [line]
    (json/decode line true)))

(def json-reducer
; JSON reducer allows to parse json files in reducible manner
; file-reducer can be used to write reducers for common formats 
; which can easily make those files transducible 
; allowing devs to create composable data processing pipelines
  (file-reducer {:record-generator parse-line :operation map}))

(defn parallel-directory-reducer [{:keys [pg-connection pipeline]}]
  (fn [{:keys [params path]}]
    (async/process-parallel pipeline [params] (files-processor path))
    (when (not= pg-connection nil)
      (db/close-connection pg-connection))))