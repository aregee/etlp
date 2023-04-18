(ns etlp.utils.mapper
  (:require [clj-http.client :as http]
            [yaml.core :as yaml]
            [jute.core :as jt]
            [cheshire.core :as json]))

(defn get-mapping
  [base-url mapping-id]
  (let [url (str base-url "/mappings/" mapping-id)]
    (try
      (let [response (http/get url)]
        (if (= 404 (:status response))
          (throw (ex-info "Mapping not found" {:status 404 :url url}))
          response))
      (catch java.net.UnknownHostException e
        (str (ex-info "Service unreachable: Unknown host" {:error e :url url})))
      (catch java.net.ConnectException e
        (str (ex-info "Service unreachable: Connection failed" {:error e :url url})))
      (catch clojure.lang.ExceptionInfo e
        (case (:status (ex-data e))
          404 (str (ex-info "Mapping not found" {:status 404 :url url}))
          (str e)))
      (catch Exception e
        (str (ex-info "Unexpected error while fetching mapping" {:error e :url url}))))))


(def parse-decoded-yaml (fn [template]
                         (yaml/parse-string template :keywords true)))

(def resolve-jute-template (fn [resp]
                              (-> resp
                                  :content
                                  :yaml
                                  parse-decoded-yaml
                                  jt/compile)))

(defn fetch-mappings [{:keys [base-url specs]}]
  (let [mappings (atom {})]
    (doseq [[alias mapping-id] specs]
      (let [response (get-mapping base-url mapping-id)]
        (if (= 200 (:status response))
          (let [ resolved-jute (resolve-jute-template (json/decode (:body response) true))]
            (swap! mappings assoc alias resolved-jute))
          (swap! mappings assoc alias (str "Error fetching mapping for alias: " alias ", mapping-id: " mapping-id ", " response)))))
    @mappings))

(def config
  {:etlp-mapper
   {:base-url "http://localhost:3000"
    :specs {:ADT-PL "13"
            :test-mapping "16"}}})

(defn main [& _args]
  (let [mappings (fetch-mappings (:etlp-mapper config))]
    mappings))
