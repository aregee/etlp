; Namespace: etlp.processors.http
(ns etlp.processors.http
  (:require [clojure.core.async :as async :refer [go-loop]]
            [cheshire.core :as json]
            [etlp.utils.reducers :refer [lines-reducible]]
            [clojure.java.io :as io]
            [clj-http.client :as http])
  (:import [java.io BufferedReader InputStreamReader ByteArrayInputStream]
           [clojure.core.async.impl.channels ManyToManyChannel]))

(defprotocol AsyncHTTP
  (start [this] "Starts a job to acquire data and returns a channel containing the Location URL")
  (check [this location-url] "Checks the status of the job using the given Location URL and returns the job status as a string")
  (list  [this location-url] "List Http Resources")
  (download [this location-url] "Downloads the data from the job using the given Location URL and returns it as a string"))

(defrecord AsyncHTTPResource [api-url headers]
  AsyncHTTP
  (start [this]
    (let [location-url (atom nil)
          channel      (async/chan)]
      (async/thread
        (try
          (let [response (http/get api-url {:headers (merge {} headers {"prefer" "respond-async"})})]
            (if (<= 200 (:status response) 299)
              (do
                (reset! location-url (get-in response [:headers "Content-Location"]))
                (println "Job started successfully.")
                (async/>!! channel {:location @location-url})
                (async/close! channel))
              (do
                (println "Job failed to start with status: " (:status response))
                (async/>!! channel {:error (str "Job failed to start with status: " (:status response))})
                (async/close! channel))))
          (catch Exception e
            (println "Failed to start job: " (.getMessage e))
            (async/>!! channel {:error (.getMessage e)})
            (async/close! channel))))
      channel))

  (check [this location-url]
    (let [status (atom nil) token (:access-token this) chan (async/chan)]
      (when-not (contains? location-url :error)
        (async/thread
         (try
          (while (not= @status 200)
            (let [response (http/get (location-url :location) {:headers headers})]
              (println "Status: " (:status response) "Progress: " (get-in response [:headers "X-Progress"]))
              (reset! status (:status response))
              (Thread/sleep 5000)))
          (async/>!! chan (condp = @status
                            200 "Job successful"
                            202 "Job still running"
                            404 "Job not found"
                            (str "Job failed with status " @status)))
          (async/close! chan)
          (catch Exception e
            (println "Failed to check job status: " (.getMessage e))
            (async/>!! chan {:error (.getMessage e)})
            (async/close! chan)))))
      chan))

  (list [this location-url]
    (let [data (atom nil) token (:access-token this) chan (async/chan)]
      (async/go
        (try
          (while (nil? @data)
            (let [response (http/get (location-url :location) {:headers headers})]
              (if (= (:status response) 200)
                (reset! data (json/decode (:body response) true)))))
          (async/>!! chan @data)
          (async/close! chan)
          (catch Exception e
            (println "Failed to download data: " (.getMessage e))
            (async/>!! chan {:error e}))))
      chan))

  (download [this location-url]
    (let [data (atom nil) token (:access-token this) chan (async/chan)]
      (async/go
        (try
          (while (nil? @data)
            (let [response (http/get (location-url :location) {:headers headers})]
              (if (= (:status response) 200)
                (reset! data  (-> response
                                     :body
                                     .getBytes
                                     ByteArrayInputStream.)))))
          (async/>!! chan @data)
          (async/close! chan)
          (catch Exception e
            (println "Failed to download data: " (.getMessage e))
            (async/>!! chan {:error e})
            (async/close! chan))))
      chan)))
