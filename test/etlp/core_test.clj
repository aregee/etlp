(ns etlp.core-test
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.core.async :as a]
            [clojure.string :as s]
            [etlp.core :as etlp]
            [etlp.utils.mapper :as mapper]
            [etlp.processors.stdin :refer [create-stdin-source!]]
            [etlp.processors.stdout :refer [create-stdout-destination!]]
            [clojure.test :refer :all]
            [integrant.core :as ig]
            [etlp.utils.core :refer [wrap-record wrap-log]]
            [clojure.tools.logging :refer [debug]]))



(defmacro def-etlp-component-test [name input output]
  `(clojure.test/deftest ~name
     (let [test-data# ~input
           _# (etlp/etlp-component test-data#)
           result# (get-in @etlp/*etl-config [:etlp.core/processors (get-in test-data# [:ctx :name])])]
       (clojure.test/is (= ~output result#)))))

(def sample-processor
  {:id  1 :component :etlp.core/processors
   :ctx {:name        "test"
         :process-fn  identity
         :etlp-config {}
         :etlp-mapper {}}})

(def-etlp-component-test adding-valid-processor
  sample-processor
  {:process-fn  identity
   :etlp-config {}
   :etlp-mapper {}})

(def-etlp-component-test adding-processor-with-missing-ctx
  {:id        1
   :component :etlp.core/processors
   :ctx       {:name "test"} }
  {:process-fn  nil
   :etlp-config nil
   :etlp-mapper {}})

(def-etlp-component-test adding-processor-with-nil-ctx
  {:id        1
   :component :etlp.core/processors
   :ctx       nil}
  {:process-fn  nil
   :etlp-config nil
   :etlp-mapper {}})


(deftest test-adding-processor-invalid-component
  (is (thrown? IllegalArgumentException
             (etlp/etlp-component {:id 1 :component :unknown}))))

(deftest test-default-method
  (is (thrown? IllegalArgumentException
             (etlp/etlp-component :default {:component :unknown}))))

(deftest test-ig-wrap-schema
  (testing "ig-wrap-schema returns a function that gives the current *etl-config"
    (do (reset! etlp/*etl-config nil)
        (reset! etlp/*etlp-app nil))
    (let [schema-fn (etlp/ig-wrap-schema {})]
      (is (fn? schema-fn))
      (is (= {:etlp.core/processors {}} (schema-fn)))
    (reset! etlp/*etl-config (schema-fn))
    (let [schema-fn (etlp/ig-wrap-schema {})]
      (is (= {:etlp.core/processors {}} (schema-fn)))))))


(deftest test-etlp-connector
  (testing "etlp-connector transforms config map correctly"
    (let [conf {:mapping-specs :dummy-mappings :config :dummy-config :options :dummy-options}
          result (#'etlp/etlp-connector conf)]
      (is (= {:etlp.core/mapper {:mapping-specs :dummy-mappings}
              :etlp.core/config {:conf :dummy-config}
              :etlp.core/options {:opts :dummy-options}
              :etlp.core/connection {:mapper (ig/ref :etlp.core/mapper)
                                     :config (ig/ref :etlp.core/config)
                                     :options (ig/ref :etlp.core/options)}
              :etlp.core/app {:connection (ig/ref :etlp.core/connection)}} result)))))

(run-tests)
