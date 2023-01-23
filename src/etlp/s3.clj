(ns etlp.s3
  (:require [clojure.core.async :as async :refer [>! chan go pipe pipeline]]
            [clojure.core.async.impl.protocols :as impl]
            [clojure.tools.logging.readable :refer [debug info warn]]
            [cognitect.aws.client.api :as aws]
            [cognitect.aws.credentials :as credentials]
            [clojure.pprint :refer [pprint]]
            [etlp.async :refer [process-parallel]] ;; [etlp.core-test :refer [hl7-xform]]
            [etlp.reducers :refer [lines-reducible]]
            [etlp.utils :refer [wrap-error wrap-log wrap-record]])

  (:import [java.io BufferedReader InputStreamReader])
  (:gen-class))


(defn s3-reducible [xf blob]
  (try
    (eduction
     xf
     (-> blob
         :Body
         InputStreamReader.
         BufferedReader.
         lines-reducible))
    (catch Exception e
      (debug e)
      (throw e))))

(comment
  (eduction
   (lines-reducible
    (BufferedReader.
     (InputStreamReader. (s3-reducible :s :Body))))))

(defn stream-file [{:keys [client bucket key]}]
  (aws/invoke client {:op :GetObject :request {:Bucket bucket :Key key}}))

(defn s3-read-lines [client config bucket-name key]
; Using this method would allow you to 
; treat each line as a transducible entity which can allow you to apply various 
; other operations that you want to apply to reach row  eg filter, transform, map, etc 
  (try
    (-> (stream-file {:client client :bucket bucket-name :key key})
        :Body
        InputStreamReader.
        BufferedReader.
        lines-reducible)
    (catch Exception e
      (debug e)
      (throw e))))

(defn s3-invoke [{:keys [region credentials] :as s3conf}]
  ; assert for aws keys validation
  (try (aws/client {:api :s3 :region region :credentials-provider (credentials/basic-credentials-provider credentials)})
       (catch Exception ex
         (debug ex)
         (throw ex))))

(defn list-objects-sync [client req]
  (let [request req]
    (-> client (aws/invoke request) (:Contents))))

(defn parallel-bucket-reducer [{:keys [pipeline s3-client s3-config]}]
  ;TODO: Take all s3 config map for aws/s3 client
  (debug s3-client)
  (fn [{:keys [bucket prefix]}]
    (try
      (process-parallel pipeline (list-objects-sync s3-client {:op :ListObjectsV2 :request {:Bucket bucket :Prefix prefix}}))
      (catch Exception e
        (debug e)
        (warn e)))))

(def pf (fn []
          (->
           (.availableProcessors (Runtime/getRuntime)))))

(defn list-objects-pipeline [{:keys [client bucket prefix output-channel error-channel aws-ch]}]
  (let [list-objects-request {:op :ListObjectsV2
                              :ch aws-ch
                              :request {:Bucket bucket :Prefix prefix}}]

    (async/pipeline-async 2 output-channel
                          (fn [acc result]
                            (let [contents (acc :Contents) pages (acc :KeyCount)]
                              (doseq [entry contents]
                                (println entry)
                                (go (>! result entry)))
                              (async/close! result)))
                          (aws/invoke-async client list-objects-request)
                          (fn [input-stream]
                            (go (>! error-channel (wrap-error {:error (.getMessage input-stream)})))))))

(defn get-object-pipeline [{:keys [client bucket files-channel output-channel error-channel]}]
  (async/pipeline-async 2
                        output-channel
                        (fn [acc res]
                          (let [content (aws/invoke-async
                                         client {:op :GetObject
                                                 :ch output-channel
                                                 :request {:Bucket bucket :Key (acc :Key)}})]

                            (go (>! res (-> content)))
                            (async/close! res)))
                        files-channel
                        (fn [input-stream]
                          (go (>! error-channel (wrap-error {:error (.getMessage input-stream)}))))))


(defn stream-files-from-s3-bucket [{:keys [client bucket prefix xform-provider params]}]
  (let [error-channel (chan)
        base-read (chan)
        files-channel (chan)
        output-channel (chan)
        stdout-channel (chan)
        transducer (xform-provider {:s3-client client :s3-config {} :bucket bucket})]

    (list-objects-pipeline
     {:client client
      :bucket bucket
      :prefix prefix
      :aws-ch base-read
      :xform-provider (fn []
                        (comp
                         (map (fn [log]
                                (log :Contents)))
                         cat))
      :output-channel files-channel
      :error-channel error-channel})

    (get-object-pipeline {:client client
                          :bucket bucket
                          :xform transducer
                          :files-channel files-channel
                          :output-channel output-channel
                          :error-channel error-channel})

    (pipeline 1 stdout-channel
              (map wrap-log)
              files-channel
              true?
              (fn [input-stream]
                (go (>! error-channel (wrap-error {:error (.getMessage input-stream)})))))

    (pipe error-channel stdout-channel)


    (pipeline
     1
     (doto (chan) (async/close!))
     (comp
      (map (fn [l] (println l) l)))
     stdout-channel
     true?
     (fn [input-stream]
       (go (>! error-channel (wrap-error {:error (.getMessage input-stream)})))))

    (async/<!!
     (pipeline (pf)
               stdout-channel
               (comp transducer (map wrap-record))
               output-channel
               true?
               (fn [input-stream]
                 (go (>! error-channel (wrap-error {:error (.getMessage input-stream)}))))))))

(def bucket-stdout-reducer
  (fn [{:keys [pipeline s3-client s3-config]}]
    (fn [{:keys [bucket prefix]}]
      (stream-files-from-s3-bucket {:client s3-client
                                    :bucket bucket
                                    :prefix prefix
                                    :params s3-config
                                    :xform-provider (fn [opts] pipeline)}))))



(comment




  (defn list-objects-async [client req]
    (let [api client
          request req]
      (async/go-loop [result (aws/invoke-async api request) contents (result :Contents) next-marker (:NextContinuationToken result)]
        (if next-marker
          (recur (aws/invoke-async api (assoc request :marker next-marker)) (result :Contents) (:NextContinuationToken result))
          (do (async/to-chan contents) (impl/close! contents))))))

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

