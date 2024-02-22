(ns etlp.connector-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [cheshire.core :as json]
            [clojure.walk :refer [keywordize-keys]]
            [clj-http.client :as http]
            [etlp.utils.mapper :as mapper]
            [etlp.connector.dag :as dag]
            [etlp.utils.core :refer [wrap-log wrap-record]]
            [clojure.core.async :as a]
            [clojure.java.io :as io])
  (:import [clojure.core.async.impl.channels ManyToManyChannel]))

(def not-nill (comp (partial not) nil?))

(def test-data [[[4 4 1 1] [1 2 3 4] [2 3 4 5 6 4] [1321 3214 241234 66234] [232 4214 281234 88234]]
                [[2 2 2 2] [3 4 5 6] [3 4 5 6 7 8] [2432 4325 352345 77345] [343 5325 392345 98345]]])


(defmacro def-dag-topology-test [name topology expected-output]
  `(clojure.test/deftest ~name
     (let [mock-topology# (atom ~topology)
           etlp-dag# (dag/build @mock-topology#)
           results-chan# (a/chan)
           error-chan# (a/chan)]
       (let [results# (a/<!! (a/into [] (get-in etlp-dag# [:etlp-output :channel])))]
         (is (= results# ~expected-output))))))

(def mock-topo {:workflow [[:processor-1 :processor-2]
                           [:processor-2 :processor-3]
                           [:processor-3 :processor-4]
                           [:processor-4 :etlp-output]]
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
                           :etlp-output {:meta    {:entity-type :processor
                                                   :processor   (fn [ch]
                                                                  (if (instance? ManyToManyChannel ch)
                                                                    ch
                                                                    (ch :channel)))}}}})




(def simple-topo {:workflow [[:processor-1 :xform-1]
                             [:xform-1 :processor-2]
                             [:processor-2 :xform-2]
                             [:xform-2 :processor-3]
                             [:processor-3 :xform-3]
                             [:processor-1 :xform-log]
                             [:processor-2 :xform-log]
                             [:processor-3 :xform-log]
                             [:xform-3 :etlp-output]]
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
                             :etlp-output {:meta {:entity-type :processor
                                                  :processor   (fn [ch]
                                                                 (if (instance? ManyToManyChannel ch)
                                                                   ch
                                                                   (ch :channel)))}}

                             :xform-1 {:meta {:partitions  16
                                              :entity-type :xform-provider
                                              :xform       (comp
                                                            (mapcat (fn [l] l))
                                                            (filter not-nill)
                                                            (map (fn [lst] (reduce + lst)))
                                                            (map #(* 2 %))
                                                            (map #(* 3 %)))}}
                             :xform-2 {:meta {:partitions  16
                                              :xform       (comp
                                                            (filter not-nill)
                                                            (filter number?)
                                                            (map #(* 2 %)))
                                              :entity-type :xform-provider}}

                             :xform-log {:meta {:partitions  16
                                              :xform       (comp
                                                            (keep identity))
                                              :entity-type :xform-provider}}
                             :xform-log-1 {:meta {:partitions  16
                                              :xform       (comp
                                                            (keep identity))
                                              :entity-type :xform-provider}}
                             :xform-log-2 {:meta {:partitions  16
                                              :xform       (comp
                                                            (keep identity))
                                              :entity-type :xform-provider}}
                             :xform-3 {:meta {:partitions  16
                                              :xform       (comp
                                                            (filter not-nill)
                                                            (filter number?)
                                                            (map #(* 3 %)))
                                              :entity-type :xform-provider}}}})


(def bad-topology {:workflow [[:etlp-input :etlp-output]]
                   :entities {:etlp-input  {:meta {:entity-type :processor
                                                   :partitions 10
                                                   :processor   (fn [data]
                                                                  (if (instance? ManyToManyChannel data)
                                                                    (a/onto-chan data [])
                                                                    (a/to-chan [1 2 3])))}}
                              :etlp-output {:meta {:entity-type :xform-provider
                                                   :partitions 10
                                                   :processor   (fn [data]
                                                                  (if (instance? ManyToManyChannel data)
                                                                    data
                                                                    (data :channel)))}}}})


(def-dag-topology-test test-complex-transform mock-topo [360 360 864 11232108 13460904 288 648 1188 15712092 17868888])

(def-dag-topology-test test-simple-transform simple-topo [360 360 864 11232108 13460904 288 648 1188 15712092 17868888])

(def-dag-topology-test test-bad-transform bad-topology [1 2 3])
