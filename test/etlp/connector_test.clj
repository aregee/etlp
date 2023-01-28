

(ns etlp.connector-test
  (:require [clojure.test :refer :all]
            [etlp.connector :as connector]
            [clojure.pprint :refer [pprint]]
            [clojure.core.async :as a])
  (:import [clojure.core.async.impl.channels ManyToManyChannel]))


(def not-nill (comp (partial not) nil?))


(def test-data [[1 1 1 1] [1 2 3 4] [2 3 4 5 6 4] [1321 3214 241234 66234] [232 4214 281234 88234]])


(def mock-topo {:workflow [[:processor-1 :xform-1]
                           [:xform-1 :xform-2]
                           [:xform-2 :xform-3]
                           [:xform-3 :processor-4]]
                :entities {:processor-1 {:channel (a/chan 1)
                                         :meta {:entity-type :processor
                                                :processor (fn [ch]
                                                             (if (instance? ManyToManyChannel ch)
                                                               (a/onto-chan ch test-data)
                                                               (a/to-chan test-data)))}}
                           :xform-1 {:channel (a/chan 1)
                                     :meta {:entity-type :processor
                                            :processor (fn [ch]
                                                         (a/pipe (ch :channel)
                                                                 (a/chan 1 (comp
                                                                            (filter not-nill)
                                                                            (map (fn [lst] (reduce + lst)))
                                                                            (map #(* 2 %))
                                                                            (map #(* 3 %))))))}}
                           :xform-2 {:channel (a/chan 1)
                                     :meta {:processor (fn [ch] (a/pipe (ch :channel)
                                                                        (a/chan 1 (comp
                                                                                   (filter not-nill)
                                                                                   (filter number?)
                                                                                   (map #(* 2 %))))))
                                            :entity-type :processor}}

                           :xform-3 {:channel (a/chan 1)
                                     :meta {:entity-type :processor
                                            :processor (fn [ch]
                                                         (a/pipe (ch :channel)
                                                                 (a/chan 1 (comp
                                                                            (filter not-nill)
                                                                            (filter number?)
                                                                            (map #(* 3 %))))))}}
                           :processor-4 {:channel (a/chan 1)
                                         :meta {:entity-type :processor
                                                :processor (fn [ch]
                                                             (if (instance? ManyToManyChannel ch)
                                                               ch
                                                               (ch :channel)))}}}})


(deftest test-connect
  (let [input-chan test-data
        output-chasn []
        topology (atom mock-topo)
        entities (@topology :entities)
        etlp-line (connector/connect @topology)]
    ;; (clojure.pprint/pprint etlp-line)

    ;; (doseq [path (:workflow @topology)]
    ;;   (let [[input-key output-key] path
    ;;         input-chan (get-in etlp-line [input-key :channel])
    ;;         output-chan (get-in etlp-line [output-key :channel])]
    ;;     ;; (pprint (nil? input-chan))
    ;;     ;; (pprint (nil? output-chan))
    ;;     (if (and (not-nill input-chan) (not-nill output-chan))
    ;;       (a/pipe input-chan output-chan))))

    (a/<!!
     (a/pipeline 1 (doto (a/chan) (a/close!))
                 (map (fn [d] (println "Invoked from pipeline" d) d))
                 (get-in etlp-line [:processor-4 :channel]) (fn [ex]
                                                              (println (str "Execetion Caught" ex)))))))