(ns etlp.s3-streamer
  (:require [cognitect.aws.client.api.s3 :as s3]
            [cognitect.aws.client.core :as aws]
            [clojure.core.async :as async :refer [go <! >! >!! pipeline]]
            [clojure.core.async.impl.protocols :as impl]))

(defn list-objects-async [bucket-name]
  (let [api (s3/s3)
        request (s3/list-objects {:bucket bucket-name})]
    (async/go-loop [result (aws/invoke-async api request) contents (:contents result) next-marker (:next-marker result)]
      (if next-marker
        (recur (aws/invoke-async api (assoc request :marker next-marker)) (:contents result) (:next-marker result))
        (do (async/to-chan contents) (impl/close! contents))))))

(defn stream-file-async [bucket-name key]
  (let [api (s3/s3)
        request (s3/get-object {:bucket bucket-name :key key})]
    (async/go (let [result (aws/invoke-async api request)]
                (async/to-chan (-> result :body .getInputStream))
                (impl/close! (-> result :body .getInputStream))))))

(defn stream-file [bucket-name key]
  (let [api (s3/s3)
        request (s3/get-object {:bucket bucket-name :key key})]
    (-> (aws/invoke api request)
        :body
        .getInputStream)))

(defn -main []
  (aws/set-credentials! {:access-key "access-key"
                         :secret-key "secret-key"})
  (let [ch (async/chan)
        objects-ch (list-objects-async "my-bucket")]
    (async/pipeline 8 ch #(stream-file-async "my-bucket" (:key %)) objects-ch)))
