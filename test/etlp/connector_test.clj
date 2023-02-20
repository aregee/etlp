(ns etlp.connector-test
  (:require [clojure.test :refer :all]
            [etlp.connector :as connector :refer (map->EtlpS3Connector)]
            [etlp.core-test :refer [hl7-xform]]
            [clojure.pprint :refer [pprint]]
            [etlp.utils :refer [wrap-log wrap-record]]
            [etlp.s3 :refer [s3-invoke s3-reducible]]
            [etlp.async :refer [save-into-database]]
            [cognitect.aws.client.api :as aws]
            [clojure.core.async :as a]
            [clojure.java.io :as io])
  (:import [clojure.core.async.impl.channels ManyToManyChannel]))


(def not-nill (comp (partial not) nil?))


(def test-data [[[1 1 1 1] [1 2 3 4] [2 3 4 5 6 4] [1321 3214 241234 66234] [232 4214 281234 88234]]
                [[2 2 2 2] [3 4 5 6] [3 4 5 6 7 8] [2432 4325 352345 77345] [343 5325 392345 98345]]])


(def s3-config {:region "us-east-1"
                :credentials {:access-key-id (System/getenv "ACCESS_KEY_ID")
                              :secret-access-key (System/getenv "SECRET_ACCESS_KEY_ID")}})


(defn list-objects-pipeline [{:keys [client bucket prefix files-channel]}]
  (let [list-objects-request {:op :ListObjectsV2 :request {:Bucket bucket :Prefix prefix}}]
    (a/go (loop [marker nil]
            (let [response   (a/<! (aws/invoke-async client list-objects-request))
                  contents   (:Contents response)
                  new-marker (:NextMarker response)]
              (doseq [file contents]
                (a/>! files-channel file))
              (if new-marker
                (recur new-marker)
                (a/close! files-channel))))
          files-channel)))


(defn get-object-pipeline-async [{:keys [client bucket files-channel output-channel]}]
  (a/pipeline-async 8
                    output-channel
                    (fn [acc res]
                      (a/go
                        (let [content (a/<! (aws/invoke-async
                                             client {:op      :GetObject
                                                     :request {:Bucket bucket :Key (acc :Key)}}))]
                          (a/>! res content)
                          (a/close! res))))
                    files-channel))

(def s3-client (s3-invoke s3-config))

(def list-s3-processor  (fn [data]
                          (list-objects-pipeline {:client        (data :s3-client)
                                                  :bucket        (data :bucket)
                                                  :files-channel (data :channel)
                                                  :prefix        (data :prefix)})
                          (data :channel)))

(def log-s3-file-paths (fn [ch]
                         (if (instance? ManyToManyChannel ch)
                           (a/pipe ch (a/chan 1 (comp
                                                 (filter not-nill)
                                                 (map wrap-log)
                                                 (keep println))))
                           (a/pipe (ch :channel) (a/chan 1 (comp
                                                            (filter not-nill)
                                                            (map wrap-log)))))))

(def reduce-s3 (comp
                (mapcat (partial s3-reducible (hl7-xform {})))
                (map wrap-record)))


(def get-s3-objects (fn [data]
                      (println "TODO:Check if instance of channel, we ned to abstract out this part")
                      (let [output (a/chan)]
                        (get-object-pipeline-async {:client         (data :s3-client)
                                                    :bucket         (data :bucket)
                                                    :files-channel  (data :channel)
                                                    :output-channel output})
                        output)))

(def etlp-processor (fn [ch]
                      (if (instance? ManyToManyChannel ch)
                        ch
                        (ch :channel))))

(def s3-processing-toology {:entities
                            {:list-s3-objects {:s3-client s3-client
                                               :bucket (System/getenv "ETLP_TEST_BUCKET")
                                               :channel (a/chan)
                                               :prefix "messages"
                                               :meta  {:entity-type :processor
                                                       :processor list-s3-processor}}

                             :get-s3-objects {:s3-client s3-client
                                              :bucket (System/getenv "ETLP_TEST_BUCKET")
                                              :meta {:entity-type :processor
                                                     :processor get-s3-objects}}

                             :processor-5 { :meta {:entity-type :processor
                                            :processor etlp-processor}}}
                            :workflow [[:list-s3-objects :get-s3-objects]
                                       [:get-s3-objects :processor-5]]})


(defn topology-builder [{:keys [s3-config prefix bucket processors]}]
  (let [s3-client (s3-invoke s3-config)
        entities  {:list-s3-objects {:s3-client s3-client
                                     :bucket    bucket
                                     :prefix    prefix
                                     :channel   (a/chan)
                                     :meta      {:entity-type :processor
                                                 :processor   (processors :list-s3-processor)}}

                   :get-s3-objects {:s3-client s3-client
                                    :bucket    bucket
                                    :meta      {:entity-type :processor
                                                :processor   (processors :get-s3-objects)}}

                   :etlp-output {:channel (a/chan)
                                 :meta    {:entity-type :processor
                                           :processor   (processors :etlp-processor)}}}
        workflow [[:list-s3-objects :get-s3-objects]
                   [:get-s3-objects :etlp-output]]]

    {:entities entities
     :workflow workflow}))

(deftest test-connect-s3
  (let [s3-connector (map->EtlpS3Connector {:s3-config        s3-config
                                            :prefix           "stormbreaker/hl7"
                                            :bucket           (System/getenv "ETLP_TEST_BUCKET")
                                            :processors       {:list-s3-processor list-s3-processor
                                                               :get-s3-objects    get-s3-objects
                                                               :etlp-processor    etlp-processor
                                                               }
                                            :reducers         {
                                                               :s3-reducer reduce-s3
                                                               }
                                            :reducer          :s3-reducer
                                            :topology-builder topology-builder})]
    (a/<!!
     (.read! s3-connector))))


(comment
  (deftest test-connect-s3
    (let [topology (atom s3-processing-toology)
          etlp (connector/connect @topology)]
      (clojure.pprint/pprint "S3 Topo")
      (a/<!!
       (a/pipeline 1 (doto (a/chan)(a/close!))
                   (comp
                    ;; reduce-s3
                    (map (fn [d] (println d) d))
                    (partition-all 1000)
                    (keep save-into-database))
                   (get-in etlp [:processor-5 :channel])
                   true
                   (fn [ex]
                     (println (str "Execetion Caught" ex))))))))

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
