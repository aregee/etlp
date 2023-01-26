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
  (:import [java.io BufferedReader InputStreamReader]
           [clojure.core.async.impl.channels ManyToManyChannel])
  (:gen-class))


(defn s3-reducible [xf blob]
  ;; (print "Should be blob" blob)
  (if (instance? ManyToManyChannel blob)
    (throw (Exception. "Invalid Blob"))
    (try
      (eduction
       xf
       (-> blob
           :Body
           InputStreamReader.
           BufferedReader.
           lines-reducible))
      (catch Exception e
        (println (str "Error Processing" e))
        (throw e)))))

(comment
  (eduction
   (lines-reducible
    (BufferedReader.
     (InputStreamReader. (s3-reducible :s :Body))))))

(defn stream-file [{:keys [s3-client bucket Key]}]
  (aws/invoke s3-client {:op :GetObject :request {:Bucket bucket :Key Key}}))

(defn s3-read-lines [{:keys [Key s3-client bucket] :as s3blob}]
; Using this method would allow you to 
; treat each line as a transducible entity which can allow you to apply various 
; other operations that you want to apply to reach row  eg filter, transform, map, etc  
  (try
    (-> (stream-file s3blob)
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
          1))




(defn list-objects-pipeline [{:keys [client bucket prefix output-channel error-channel aws-ch]}]
  (let [list-objects-request {:op :ListObjectsV2
                              :ch output-channel
                              :request {:Bucket bucket :Prefix prefix}}]

    (async/pipeline-async 1 output-channel
                          (fn [acc result]
                            (let [contents (acc :Contents) pages (acc :KeyCount)]
                              (go
                                (doseq [entry contents]
                                  (>! result entry))
                                (async/close! result))))
                          (aws/invoke-async client list-objects-request))))

(defn get-object-pipeline [{:keys [client bucket files-channel output-channel file-channel]}]
  (async/pipeline-async 6
                        file-channel
                        (fn [acc res]
                          (let [content (aws/invoke
                                         client {:op :GetObject
                                                 :request {:Bucket bucket :Key (acc :Key)}})]
                            (go (>! res content)
                                (async/close! res))))
                        files-channel))


(defn stream-files-from-s3-bucket [{:keys [client bucket prefix xform-provider params]}]
  (let [error-channel (chan)
        base-read (chan)
        files-channel (chan)
        input-stream (chan)
        output-channel (chan)
        stdout-channel (chan)
        transducer (xform-provider {:s3-client client :s3-config {} :bucket bucket})]






    (async/<!! (pipeline 
                1
                (doto (chan) (async/close!))
                (comp
                 (map (fn [l] (println l) l)))
                (pipe
                 (pipeline
                  6
                  stdout-channel
                  (comp transducer (map wrap-record))
                  (pipe
                   (pipe (list-objects-pipeline
                          {:client client
                           :bucket bucket
                           :prefix prefix
                           :aws-ch base-read
                           :output-channel files-channel
                           :error-channel error-channel})
                         (get-object-pipeline {:client client
                                               :bucket bucket
                                               :xform transducer
                                               :files-channel files-channel
                                               :file-channel input-stream}))
                   (pipe input-stream output-channel))
                  false
                  (fn [error-stream]
                    (go (>! error-channel (wrap-error {:error (str ">>Exception:." error-stream)})))))
                 (pipe error-channel
                       (pipe (pipeline
                              1
                              stdout-channel
                              (map wrap-log)
                              files-channel) stdout-channel)))
                false
                (fn [error-stream]
                  (go (>! error-channel (wrap-error {:error (.getMessage error-stream)}))))))))

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

  ;; (def s3-file-reducer (fn [client filepath]
  ;;                        (eduction
                          ;; (s3-read-lines client {} "test-dev-env" filepath))))

  ;; (defn s3-directory-processor [client s3-list-request]
  ;;   (let [s3-reducer (partial s3-file-reducer s3-invoke)]
  ;;     (process-parallel (comp
  ;;                           ;; (mapcat (fn [l] (l :Contents)))
  ;;                        (keep (fn [l] (l :Key)))
  ;;                        (mapcat s3-reducer)
  ;;                        (keep (fn [l] (info l) l)))
  ;;                       (aws/invoke-async client {:op :ListObjectsV2 :request s3-list-request}))))

  (defn logger [log]
    (log :Contents)))

