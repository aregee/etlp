; Namespace: etlp.processors.http
(ns etlp.http
  (:require [clojure.core.async :as async]
            [clj-http.client :as http]))

(defn- paginated-request [uri headers limit offset]
  (let [response (http/get uri {:headers headers :query-params {:limit limit :offset offset}})
        total (-> response :headers (get "x-total-count") Integer/parseInt)
        data (-> response :body (json/read-str) :data)
        count (count data)
        next-offset (+ offset count)]
    {:data data :total total :count count :offset offset :next-offset next-offset}))

(defn- async-request [uri headers]
  (let [response-chan (async/chan)]
    (http/get uri {:headers headers}
               (fn [response]
                 (async/>! response-chan response)))
    response-chan))

(defn- download-file [uri output-stream]
  (http/get uri {:as :stream}
             (fn [{:keys [status headers body]}]
               (when (= status 200)
                 (with-open [output output-stream]
                   (io/copy body output))))))

(defprotocol PaginatedResource
  (start [this uri headers limit])
  (stop [this]))

(defprotocol AsyncResource
  (start [this uri headers])
  (stop [this]))

(defrecord HttpPaginatedResource [uri headers]
  PaginatedResource
  (start [this uri headers limit]
    (let [result-chan (async/chan)]
      (go-loop [offset 0]
        (when (not= offset -1)
          (let [{:keys [data total count offset next-offset]} (paginated-request uri headers limit offset)]
            (doseq [datum data]
              (async/>! result-chan datum))
            (if (< next-offset total)
              (do
                (Thread/sleep 1000) ;; delay before next request
                (recur next-offset))
              (async/close! result-chan)
              -1))))
      result-chan))
  (stop [this]))

(defrecord HttpAsyncResource [uri headers]
  AsyncResource
  (start [this uri headers]
    (let [response-chan (async-request uri headers)]
      (go-loop []
        (when-let [response (async/<! response-chan)]
          (when (not= (:status response) 200)
            (Thread/sleep 1000) ;; delay before next request
            (recur)))
        response-chan)))
  (stop [this]))

(defrecord FileDownloadResource [uri output-stream]
  (start [this]
    (download-file uri output-stream))
  (stop [this]))
