(ns etlp.mapper-sdk
  (:require [clj-http.client :as http]
            [yaml.core :as yaml]
            [jute.core :as jt]
            [cheshire.core :as json]))

(defn get-mapping [base-url mapping-id]
  (let [url (str base-url "/mappings/" mapping-id)]
    (http/get url)))

(def parse-decoded-yaml (fn [template]
                         (yaml/parse-string template :keywords true)))

(def resolve-jute-template (fn [resp]
                              (-> resp
                                  :content
                                  :yaml
                                  parse-decoded-yaml
                                  jt/compile)))

(defn fetch-mappings [config]
  (let [base-url (:base-url config)
        specs (:specs config)
        mappings (atom {})]
    (doseq [[alias mapping-id] specs]
      (let [response (get-mapping base-url mapping-id)]
        (if (= 200 (:status response))
          (let [ resolved-jute (resolve-jute-template (json/decode (:body response) true))]
            (swap! mappings assoc alias resolved-jute))
          (println (str "Error fetching mapping for alias: " alias ", mapping-id: " mapping-id)))))
    @mappings))

(def config
  {:etlp-mapper
   {:base-url "http://localhost:3000"
    :specs {:ADT-PL "13"
            :test-mapping "16"}}})

(defn main [& _args]
  (let [mappings (fetch-mappings (:etlp-mapper config))]
    mappings))
