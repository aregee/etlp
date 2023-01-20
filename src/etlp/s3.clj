(ns etlp.s3
  (:require [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as impl]
            [clojure.pprint :refer [pprint]]
            [clojure.tools.logging.readable :refer [debug info warn]]
            [cognitect.aws.client.api :as aws]
            [cognitect.aws.credentials :as credentials]
            [etlp.async :refer [process-parallel]]
            [etlp.reducers :refer [lines-reducible]])
  (:import [java.io BufferedReader InputStreamReader])
  (:gen-class))

(defn stream-file [{:keys [client bucket key]}]
  (aws/invoke client {:op :GetObject :request {:Bucket bucket :Key key}}))

(defn s3-read-lines [client config bucket-name key]
; Using this method would allow you to 
; treat each line as a transducible entity which can allow you to apply various 
; other operations that you want to apply to reach row  eg filter, transform, map, etc 
  (-> (stream-file {:client client :config config :bucket bucket-name :key key})
      :Body
      InputStreamReader.
      BufferedReader.
      lines-reducible))

(defn s3-invoke [{:keys [region credentials] :as s3conf}]
  ; assert for aws keys validation
  (debug "in s3-invokkede")
  (try (aws/client {:api :s3 :region region :credentials-provider (credentials/basic-credentials-provider credentials)})
       (catch Exception ex
         (debug (.printStackTrace ex))
         (warn (str "caught exception: " (.getMessage ex))))))

(defn list-objects-sync [client req]
  (let [request req]
    (-> client (aws/invoke request) (:Contents))))

(defn list-objects-async [client req]
  (let [api client
        request req]
    (async/go-loop [result (aws/invoke-async api request) contents (result :Contents) next-marker (:NextContinuationToken result)]
      (if next-marker
        (recur (aws/invoke-async api (assoc request :marker next-marker)) (result :Contents) (:NextContinuationToken result))
        (do (async/to-chan contents) (impl/close! contents))))))

(defn parallel-bucket-reducer [{:keys [pipeline s3-client s3-config]}]
  ;TODO: Take all s3 config map for aws/s3 client
  (debug s3-client)
  (fn [{:keys [bucket prefix]}]
    (process-parallel pipeline (list-objects-sync s3-client {:op :ListObjectsV2 :request {:Bucket bucket :Prefix prefix}}))))


(comment
  (defn process-with-transducers [xf files]
    (transduce
     xf
     (constantly nil)
     nil
     files))

  (def s3-file-reducer (fn [client filepath]
                         (eduction
                          (s3-read-lines client {} "test-dev-env" filepath))))

  (defn s3-directory-processor [client s3-list-request]
    (let [s3-reducer (partial s3-file-reducer s3-invoke)]
      (process-parallel (comp
                            ;; (mapcat (fn [l] (l :Contents)))
                         (keep (fn [l] (l :Key)))
                         (mapcat s3-reducer)
                         (keep (fn [l] (info l) l)))
                        (aws/invoke-async client {:op :ListObjectsV2 :request s3-list-request}))))

  (defn logger [log]
    (log :Contents)))

