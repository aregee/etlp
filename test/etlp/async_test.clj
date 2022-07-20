(ns etlp.async-test
  (:require [clojure.test :refer :all]
            [clojure.string :as s]
            [etlp.core :as etlp]))

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
  (-> line
      (s/split #"\|")
      (fields-for-procedure)))

(defn parse-line [_]
  valid-logs-only?)


(def parse-procedure-file (etlp/file-reducer {:record-generator parse-line :operation map}))


(def dummy (atom []))

(defn save-into-database [batch]
  ;; (swap! rows + (count batch))
  (swap! dummy concat batch))

(defn- pipeline [_]
  (comp (mapcat parse-procedure-file)    ;; Pipeline transducer
        (map record-generator-procedure)
        (partition-all 100)
        (map save-into-database)))

;; (def procedure-processor (etlp/create-pipeline-processor {:pg-connection nil :pipeline pipeline}))

;; (defn exec-inno-procedure [{:keys [path days]}]
;;   (procedure-processor {:params days :path path}))


;; (deftest e-to-e-test
;;   (testing "etlp/create-pipeline-processor should execute without error"
;;     (is (= nil (exec-inno-procedure {:path "resources/procedure/" :days 1})))
;;     ;; (is (= 6000 (count @dummy)))))

;; (deftest e-to-e-count
;;   (testing "etlp/create-pipeline-processor should produce 6000 records"
;;     (is (= 6000 (count @dummy)))))