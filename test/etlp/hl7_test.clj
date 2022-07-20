(ns etlp.hl7-test
  (:require [clojure.test :refer :all]
            [com.nervestaple.hl7-parser.parser :as parser]
            [etlp.core :as etlp]
            [clojure.java.io :as io]
            [clojure.string :as s]
            [etlp.reducers :refer [read-lines]]))


(defn- valid-logs-only? [line]
  line)


(defn parse-line [_]
  valid-logs-only?)


(defn record-start? [log-line]
  (.startsWith log-line "MSH"))


(defn next-log-record [hl7-lines]
  (let [head (first hl7-lines)
        body (take-while (complement record-start?) (rest hl7-lines))]
    (remove nil? (conj body head))))


(defn- lazy-hl7-xform
  "Returns a lazy sequence of lists like partition, but may include
  partitions with fewer than n items at the end.  Returns a stateful
  transducer when no collection is provided."
  ([^long n]
   (fn [rf]
     (let [a (java.util.ArrayList.)]
       (fn
         ([] (rf))
         ([result]
          (let [result (if (.isEmpty a)
                         result
                         (let [v (vec (.toArray a))]
                             ;;clear first!
                           (.clear a)
                           (unreduced (rf result v))))]
            (rf result)))
         ([result input]
          (.add a input)
          (if (and (> (count a) 1) (= true (record-start? input)))
            (let [v (vec (.toArray a))]
              (.clear a)
              (.add a (last v))
              (rf result (drop-last v)))
            result))))))

  ([_ log-lines]
   (lazy-seq
    (when-let [s (seq log-lines)]
      (let [record (doall (next-log-record s))]
        (cons record
              (lazy-hl7-xform (count record) (nthrest s (count record)))))))))


(def dummy (atom []))


(defn save-into-database [batch]
  ;; (swap! dummy + (count batch))
  (swap! dummy concat [batch]))


(defn file-reducer [{:keys [record-generator operation]}]
  (fn [filepath]
    (prn filepath)
    (eduction
     (lazy-hl7-xform 0)
     (read-lines filepath))))


(def parse-procedure-file (file-reducer {:record-generator parse-line :operation map}))


(defn build-msg [vect]
  ;; (prn (count vect))
  (s/join "\r" vect))


(defn logger [line]
  (clojure.pprint/pprint "ETLP-Logger[DEBUG] :==")
  (clojure.pprint/pprint (record-start? line)))


(defn- pipeline [_]
  (comp (mapcat parse-procedure-file)
        (map build-msg)
        (map parser/parse)
        (partition-all 5)
        (map save-into-database)))

(def procedure-processor (etlp/create-pipeline-processor {:pg-connection nil :pipeline pipeline}))


(defn exec-inno-procedure [{:keys [path days]}]
  (procedure-processor {:params days :path path}))


(deftest e-to-e-test
  (testing "etlp/create-pipeline-processor should execute without error"
    (is (= nil (exec-inno-procedure {:path "resources/hl7/" :days 1})))
    (is (= 26 (count @dummy)))))
