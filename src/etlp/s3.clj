(ns etlp.s3
  (:require [clojure.core.async.impl.protocols :as impl]
            [clojure.pprint :refer [pprint]]
            [clojure.tools.logging.readable :refer [debug info warn]]
            [cognitect.aws.client.api :as aws]
            [cognitect.aws.credentials :as credentials]
            [clojure.core.async :as async :refer [<! >! chan go pipe pipeline]]
            [cheshire.core :as json]
            [clojure.string :as s]
            [clojure.java.io :as io]
            [etlp.async :refer [process-parallel]]
            [etlp.reducers :refer [lines-reducible]])
  (:import [java.io BufferedReader InputStreamReader])
  (:gen-class))


(defn record-start? [log-line]
  (.startsWith log-line "MSH"))

(def invalid-msg? (complement record-start?))

(defn is-valid-hl7? [msg]
  (cond-> []
    (invalid-msg? msg) (conj "Message should start with MSH segment")
    (< (.length msg) 8) (conj "Message is too short (MSH truncated)")))

(defn next-log-record [ctx hl7-lines]
  (let [head (first hl7-lines)
        body (take-while (complement record-start?) (rest hl7-lines))]
    (remove nil? (conj body head))))

(defn hl7-xform
  "Returns a lazy sequence of lists like partition, but may include
  partitions with fewer than n items at the end.  Returns a stateful
  transducer when no collection is provided."
  ([ctx]
   (fn [rf]
     (let [a (java.util.ArrayList.)]
       (fn
         ([] (rf))
         ([result]
          (let [result (if (.isEmpty a)
                         result
                         (let [v (vec (.toArray a))]
                             ;;clear first!
                           (.clear a)
                           (unreduced (rf result v))))]
            (rf result)))
         ([result input]
          (.add a input)
          (if (and (> (count a) 1) (= true (record-start? input)))
            (let [v (vec (.toArray a))]
              (.clear a)
              (.add a (last v))
              (rf result (drop-last v)))
            result))))))

  ([ctx log-lines]
   (lazy-seq
    (when-let [s (seq log-lines)]
      (let [record (doall (next-log-record ctx s))]
        (cons record
              (hl7-xform ctx (nthrest s (count record)))))))))

(defn stream-file [{:keys [client bucket key]}]
  (aws/invoke client {:op :GetObject :request {:Bucket bucket :Key key}}))

(defn s3-read-lines [client config bucket-name key]
; Using this method would allow you to 
; treat each line as a transducible entity which can allow you to apply various 
; other operations that you want to apply to reach row  eg filter, transform, map, etc 
  (-> (stream-file {:client client :bucket bucket-name :key key})
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

(defn wrap-data [data type]
  (let [wrapped-data {:type type
                      :timestamp (System/currentTimeMillis)
                      :version "0.1.0"
                      :schema "hl7_raw" ;{"fields" [{"name" "field1" "type" "string"} {"name" "field2" "type" "integer"}] "primary_key" ["field1"]}
                      :source_stream "hl7-stream"
                      :data data}]
    (json/encode wrapped-data)))

(defn wrap-record [data]
  (wrap-data data :record))

(defn wrap-error [data]
  (wrap-data data :error))

(defn wrap-log [data]
  (wrap-data data :log))

(def pf (fn []
          (->
           (.availableProcessors (Runtime/getRuntime))
           dec
           dec)))

(defn list-objects-pipeline [{:keys [client bucket prefix output-channel xform-provider error-channel aws-ch]}]
  (let [list-objects-request {:op :ListObjectsV2
                              :ch aws-ch
                              :request {:Bucket bucket :Prefix prefix}}]

    (async/pipeline (pf) output-channel
                    (xform-provider)
                    (aws/invoke-async client list-objects-request)
                    true
                    (fn [input-stream]
                      (go (>! error-channel (wrap-error {:error (.getMessage input-stream)})))))))

(defn get-object-pipeline [{:keys [client bucket files-channel output-channel error-channel]}]
  (pipeline (pf)
            output-channel
            (mapcat (fn [file]
                      (eduction
                       (hl7-xform {})
                      ;;  (filter is-valid-hl7?)
                       (s3-read-lines client {} bucket file))))

            files-channel
            true
            (fn [input-stream]
              (go (>! error-channel (wrap-error {:error (.getMessage input-stream)}))))))

(def log-record (comp println wrap-log))


(defn stream-files-from-s3-bucket [{:keys [client bucket prefix xform-provider params]}]
  (let [error-channel (chan)
        base-read (chan)
        files-channel (chan)
        output-channel (chan)
        ;; stdout (chan)
        get-object-opts {:client client
                         :bucket bucket
                         :files-channel files-channel
                         :output-channel output-channel
                         :error-channel error-channel}
        transducer (xform-provider params)]

    (list-objects-pipeline
     {:client client
      :bucket bucket
      :prefix prefix
      :aws-ch base-read
      :xform-provider (fn []
                        (comp
                         (take-while (fn [item]
                                       (if (= (count (item :Contents)) (item :KeyCount))
                                         (do (async/close! base-read) true)
                                         true)))
                         (map (fn [log]
                                (log :Contents)))
                         cat
                         transducer
                         (map (fn [path]
                                (log-record path)
                                path))))
      :output-channel files-channel
      :error-channel error-channel})

    (get-object-pipeline get-object-opts)

    (async/<!!
     (pipeline
      (pf)
      (doto (async/chan) (async/close!))
      (comp
      ;;  (map (fn [log] (json/decode log)))
       (map wrap-record)
       (map (fn [l]
              (println l)
              l)))
      output-channel
      true
      (fn [input-stream]
        (go (>! error-channel (wrap-error {:error (.getMessage input-stream)}))))))

    (go (while true
          (let [log (<! error-channel)]
            (println log))))))



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

