(ns etlp.hl7-test
  (:require [clojure.test :refer :all]
            [com.nervestaple.hl7-parser.parser :as parser]
            [etlp.core :as etlp]
            [clojure.string :as s]
            [etlp.reducers :refer [read-lines]]))

(def db-config {:host "localhost"
                :user "test"
                :dbname "test"
                :password "test"
                :port 5432})

(defn procedure-schema []
  [[:id :serial "PRIMARY KEY"]
   [:practice :varchar]
   [:provider :varchar]
   [:additional_procedure :varchar]
   [:betos_code :varchar]
   [:birth_date :varchar]
   [:encounter_id :varchar]
   [:end_datetime :varchar]
   [:first_name :varchar]
   [:gender :varchar]
   [:last_name :varchar]
   [:local_member_id :varchar]
   [:primary_procedure_code :varchar]
   [:primary_procedure_coding_system :varchar]
   [:primary_procedure_modifier_code :varchar]
   [:primary_procedure_name :varchar]
   [:primary_procedure_status :varchar]
   [:primary_procedure_type :varchar]
   [:procedure_id :varchar]
   [:procedure_note :varchar]
   [:source_file_name :varchar]
   [:source_record_date :varchar]
   [:start_datetime :varchar]
   [:surgical_history_flag :varchar]])

(def table-opts {:table :inno_procedure_entries
                 :specs  (procedure-schema)})

(def fields-list [:practice :provider :additional_procedure :betos_code :birth_date :encounter_id :end_datetime :first_name :gender :last_name :local_member_id :primary_procedure_code :primary_procedure_coding_system :primary_procedure_modifier_code :primary_procedure_name :primary_procedure_status :primary_procedure_type :procedure_id :procedure_note :source_file_name :source_record_date :start_datetime :surgical_history_flag])

(defn- fields-for-procedure [& values]
  ;; (print values)
  (if (> (count (first values)) 0)
    (zipmap fields-list (first values))
    (zipmap fields-list  [:nill :nill :nill :nill :nill :nill :nill :nill :nill :nill :nill :nill :nill :nill :nill :nill :nill :nill :nill :nill :nill :nill :nill])))


(defn- record-generator-procedure [line]
  ;; (prn line)
  (merge (fields-for-procedure)
         line))


(defn- valid-logs-only? [line]
  line)

(defn parse-line [_]
  valid-logs-only?)


(defn record-start? [log-line]
  (.startsWith log-line "MSH"))

;; (defn is-dx-error? [line]
;;   (.startsWith line "/*** DX Error"))

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
     (let [a (java.util.ArrayList. n)]
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
              (rf result v))
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
  (swap! dummy concat [batch])
  )

(defn file-reducer [{:keys [record-generator operation]}]
  (fn [filepath]
    (prn filepath)
    (eduction
     (lazy-hl7-xform 14749)
     (read-lines filepath))))


(def parse-procedure-file (file-reducer {:record-generator parse-line :operation map}))

(defn build-msg [vect] 
  ;; (prn (count vect))
  (s/join #"" vect))

(defn logger [line]
  (clojure.pprint/pprint "ETLP-Logger[DEBUG] :==")
  (clojure.pprint/pprint (build-msg line)))

(defn- pipeline [_]
  (comp (mapcat parse-procedure-file)
        (map build-msg)
        ;; (mapcat parser/parse)
        ;; (map parser/parse)
        ;; (map logger)
        ;; Pipeline transducer
        ;; (map record-generator-procedure)
        ;; (mapcat parser/parse)
        ;; (partition-all 5)
        ;; (mapcat parser/parse)
        (map save-into-database)))

(def procedure-processor (etlp/create-pipeline-processor {:pg-connection nil :pipeline pipeline}))

(defn exec-inno-procedure [{:keys [path days]}]
  (procedure-processor {:params days :path path}))


(defn parse-all [data]
  (map parser/parse data))

(deftest e-to-e-test
  (testing "etlp/create-pipeline-processor should execute without error"
    (is (= nil (exec-inno-procedure {:path "resources/hl7/" :days 1})))
    (is (= (count @dummy) 10))))

;; (deftest e-to-e-count
;;   (testing "etlp/create-pipeline-processor should produce 6000 records"
;;     (is (> (count @dummy) 1))))