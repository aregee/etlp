(ns etlp.connector-test
  (:require [clojure.test :refer :all]
            [etlp.core-test :refer [hl7-xform]]
            [clojure.pprint :refer [pprint]]
            [cheshire.core :as json]
            [clojure.walk :refer [keywordize-keys]]
            [clj-http.client :as http]
            [etlp.mapper-sdk :as mapper]
            [etlp.utils :refer [wrap-log wrap-record]]
            [etlp.s3 :refer [create-s3-source! create-s3-list-source!]]
            [etlp.db :refer [create-postgres-source!]]
            [etlp.http :refer (->AsyncHTTPResource)]
            [etlp.connector :refer [create-stdout-destination! create-connection etlp-source etlp-destination]]
            [etlp.async :refer [save-into-database]]
            [cognitect.aws.client.api :as aws]
            [clojure.core.async :as a]
            [clojure.java.io :as io])
  (:import [clojure.core.async.impl.channels ManyToManyChannel]))

(def not-nill (comp (partial not) nil?))

(def test-data [[[4 4 1 1] [1 2 3 4] [2 3 4 5 6 4] [1321 3214 241234 66234] [232 4214 281234 88234]]
                [[2 2 2 2] [3 4 5 6] [3 4 5 6 7 8] [2432 4325 352345 77345] [343 5325 392345 98345]]])

(def s3-config {:region "us-east-1"
                :credentials {:access-key-id (System/getenv "ACCESS_KEY_ID")
                              :secret-access-key (System/getenv "SECRET_ACCESS_KEY_ID")}})


(def etlp-s3-source {:s3-config s3-config
                     :bucket (System/getenv "ETLP_TEST_BUCKET")
                     :prefix "stormbreaker/hl7"
                     :reducers {:hl7-reducer
                                (comp
                                 (hl7-xform {})
                                 (map (fn [segments]
                                        (clojure.string/join "\r" segments))))}
                     :reducer :hl7-reducer})

(def db-spec {:dbtype "postgresql"
              :dbname "test"
              :user "postgres"
              :password "postgres"
              :host "localhost"
              :port 5432})

(def query "SELECT * FROM test_log_clj")

(def page-size 1000)

(def poll-interval 100)

(def offset-atom (atom 0))

(def jdbc-process-opts {:db-spec       db-spec
                        :query         query
                        :page-size     page-size
                    :poll-interval poll-interval
                        :offset-atom   offset-atom})


(def reducer-sets {:json-reducer (comp (map (fn [data]
                                              (get-in (keywordize-keys data) [:json_build_object :results])))
                                       (mapcat (fn [item] (keywordize-keys item))))})

(def etlp-pg-source {:db-config jdbc-process-opts
                     :reducers reducer-sets
                     :reducer :json-reducer})


;; Read all results from the channel

(def connect-etlp-pg {:xform       (comp (map wrap-record))
                      :xform-provider (fn [{:keys [mapper]}]
                                        (comp
                                         (map wrap-record)))
                      :threads     16
                      :source      (create-postgres-source! etlp-pg-source)
                      :destination (create-stdout-destination! {})})


(def connect-etlp-s3 {:xform       (comp (map wrap-record))
                      :threads     16
                      :source      (create-s3-source! etlp-s3-source)
                      :destination (create-stdout-destination! {})})

(defn th [opts]
  (-> (create-connection opts)
      .start))

(defn ffuture [] (future (.start (Thread.  #(th connect-etlp-pg))) (.getId (Thread/currentThread))))



(def bcda-creds {:clientId (System/getenv "BCDA_USER")
                 :clientSecret (System/getenv "BCDA_SECRET")})

(defn auth-request [{:keys [clientId clientSecret]}]
  (let [url "https://sandbox.bcda.cms.gov/auth/token"
        headers {"accept" "application/json"}]
    (http/post url {:basic-auth [clientId clientSecret]
                    :headers headers
                    :body ""})))

(def bcda-sandbox-token (fn [payload]
                          (let [resp (json/decode  (payload :body) true)]
                            (resp :access_token))))

(defn header-opts [] {"Authorization" (str "Bearer " (bcda-sandbox-token (auth-request bcda-creds)))
                      "accept"        "application/fhir+json"})

(defn do-req [headers]
  (a/go (let [my-resource  (->AsyncHTTPResource "https://sandbox.bcda.cms.gov/api/v1/Patient/$export" headers)
              location-url (a/<! (.start my-resource))
              job-status   (a/<! (.check my-resource location-url))]
  (println job-status) ; should print "Job still running" or "Job successful" depending on the API's response
  (when (= job-status "Job successful")
    (let [data (a/<!(.download my-resource location-url))]
      (pprint data))))))

(deftest test-etlp-connection
   (is (= nil (th connect-etlp-s3))))


(def sample-payload-yaml "
  book:
    author:
        name: M. Soloviev
        title: PHD
        gender: m
    title: Approach to Cockroach
    chapters:
    - type: preface
      content: A preface chapter
    - type: content
      content: Chapter 1
    - type: content
      content: Chapter 2
    - type: content
      content: Chapter 3
    - type: afterwords
      content: Afterwords")

(def expected-output "
    type: book
    author: M. Soloviev
    title: Approach to Cockroach
    content:
    - Chapter 1
    - Chapter 2
    - Chapter 3")


(def hl7-raw "PID:
  '0': PID
  '1': '1'
  '2':
  '3':
  - '0': \"^\"
    '1': F8457
    '2':
    '3':
    '4':
    '5': VIT
  - '0': \"^\"
    '1': '12345567899'
    '2':
    '3':
    '4': ORCA MRN
    '5': ORCA MRN
  '4':
  '5':
    '0': \"^\"
    '1': ORCASRC
    '2': TESTSIX
  '6':
  '7': '1987-06-03T00:00:00.000Z'
  '8': M
  '9': []
  '10':
  '11':
  - '0': \"^\"
    '1': 123 WATERMELON AVE
    '2':
    '3': TESTVILLE
    '4': TN
    '5': '12345'
    '6': USA
    '7': P
    '8':
    '9': TESTVILLE
  '12': TESTVILLE
  '13':
  - '0': \"^\"
    '1': \"(785)333-3333\"
    '2': P
    '3': H
    '4':
    '5':
    '6': '785'
    '7': '2546658'
  '14': []
  '15':
  '16': []
  '17':
  '18':
    '0': \"^\"
    '1': '123456789'
  '19': 555-55-5555
  '20':
  '21':
  '22':
  '23':
  '24':
  '25':
  '26':
  '27':
  '28':
  '29':
  '30': N
  '31':
  '32':")


(def fhir-resource-example "resourceType: Patient
name:
  - given:
      - ORCASRC
    family: TESTSIX
birthDate: '1987-06-03T00:00:00.000Z'
gender: male
address:
  - line:
      - 123 WATERMELON AVE
      - null
    use: home
    city: TESTVILLE
    state: TN
    country: USA
    postalCode: '12345'
telecom:
  - use: home
    value: (785)333-3333
")


(def resolved-mapper-templates (mapper/main))


(def test-mapper-tpl (resolved-mapper-templates :test-mapping))

(deftest mapping-jute-resolved
  (is (= (mapper/parse-decoded-yaml expected-output) (test-mapper-tpl (mapper/parse-decoded-yaml sample-payload-yaml)))))





(def mock-topo {:workflow [[:processor-1 :processor-2]
                           [:processor-2 :processor-3]
                           [:processor-3 :processor-4]
                           [:processor-4 :processor-5]]
                :entities {:processor-1 {:channel (a/chan 1)
                                         :meta    {:entity-type :processor
                                                   :processor   (fn [ch]
                                                                  (if (instance? ManyToManyChannel ch)
                                                                    (a/onto-chan ch test-data)
                                                                    (a/to-chan test-data)))}}
                           :processor-2 {:channel (a/chan 1)
                                         :meta    {:entity-type :processor
                                                   :processor   (fn [ch]
                                                                  (a/pipe (ch :channel)
                                                                          (a/chan 1 (comp
                                                                                     (mapcat (fn [l] l))
                                                                                     (filter not-nill)
                                                                                     (map (fn [lst] (reduce + lst)))
                                                                                     (map #(* 2 %))
                                                                                     (map #(* 3 %))))))}}
                           :processor-3 {:channel (a/chan 1)
                                         :meta    {:processor   (fn [ch] (a/pipe (ch :channel)
                                                                                 (a/chan 1 (comp
                                                                                            (filter not-nill)
                                                                                            (filter number?)
                                                                                            (map #(* 2 %))))))
                                                   :entity-type :processor}}

                           :processor-4 {:channel (a/chan 1)
                                         :meta    {:entity-type :processor
                                                   :processor   (fn [ch]
                                                                  (a/pipe (ch :channel)
                                                                          (a/chan 1 (comp
                                                                                     (filter not-nill)
                                                                                     (filter number?)
                                                                                     (map #(* 3 %))))))}}
                           :processor-5 {:channel (a/chan 1)
                                         :meta    {:entity-type :processor
                                                   :processor   (fn [ch]
                                                                  (if (instance? ManyToManyChannel ch)
                                                                    ch
                                                                    (ch :channel)))}}}})

;; (deftest test-connect-processors
;;   (let [topology (atom mock-topo)
;;         entities (@topology :entities)
;;         etlp-line (connector/connect @topology)]
;;     (clojure.pprint/pprint "Case One Begins")
;;     (a/<!!
;;      (a/pipeline 1 (doto (a/chan) (a/close!))
;;                  (map (fn [d] (println "Invoked from pipeline" d) d))
;;                  (get-in etlp-line [:processor-5 :channel]) (fn [ex]
;;                                                               (println (str "Execetion Caught" ex)))))))

(def simple-topo {:workflow [[:processor-1 :xform-1]
                             [:xform-1 :processor-2]
                             [:processor-2 :xform-2]
                             [:xform-2 :processor-3]
                             [:processor-3 :xform-3]
                             [:xform-3 :processor-4]]
                  :entities {:processor-1 {:meta {:entity-type :processor
                                                  :processor   (fn [ch]
                                                                 (if (instance? ManyToManyChannel ch)
                                                                   (a/onto-chan ch test-data)
                                                                   (a/to-chan test-data)))}}
                             :processor-2 {:meta {:entity-type :processor
                                                  :processor   (fn [ch]
                                                                 (if (instance? ManyToManyChannel ch)
                                                                   ch
                                                                   (ch :channel)))}}

                             :processor-3 {:meta {:entity-type :processor
                                                  :processor   (fn [ch]
                                                                 (if (instance? ManyToManyChannel ch)
                                                                   ch
                                                                   (ch :channel)))}}
                             :processor-4 {:meta {:entity-type :processor
                                                  :processor   (fn [ch]
                                                                 (if (instance? ManyToManyChannel ch)
                                                                   ch
                                                                   (ch :channel)))}}

                             :xform-1 {:meta {:entity-type :xform-provider
                                              :xform       (comp
                                                            (mapcat (fn [l] l))
                                                            (filter not-nill)
                                                            (map (fn [lst] (reduce + lst)))
                                                            (map #(* 2 %))
                                                            (map #(* 3 %)))}}
                             :xform-2 {:meta {:xform       (comp
                                                            (filter not-nill)
                                                            (filter number?)
                                                            (map #(* 2 %)))
                                              :entity-type :xform-provider}}
                             :xform-3 {:meta {:xform       (comp
                                                            (filter not-nill)
                                                            (filter number?)
                                                            (map #(* 3 %)))
                                              :entity-type :xform-provider}}}})

;; (deftest test-connect-xform-and-processors
;;   (let [topology (atom simple-topo)
;;         etlp-line (connector/connect @topology)]
;;     (clojure.pprint/pprint "Case 2 begins")
;;     (a/<!!
;;      (a/pipeline 1 (doto (a/chan) (a/close!))
;;                  (map (fn [d] (println "Invoked from ETLP pipeline ::\n" d) d))
;;                  (get-in etlp-line [:processor-4 :channel]) (fn [ex]
;;                                                               (println (str "Execetion Caught" ex)))))))

(comment
  (doseq [path (:workflow {:topology mock-topo})]
    (let [[input-key output-key] path
          input-chan (get-in {} [input-key :channel])
          output-chan (get-in {} [output-key :channel])]
        ;; (pprint (nil? input-chan))
        ;; (pprint (nil? output-chan))
      (if (and (not-nill input-chan) (not-nill output-chan))
        (a/pipe input-chan output-chan)))))
