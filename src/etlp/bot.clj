(ns etlp.bot
  (:require [airbyte.protocol :as airbyte]
            [clojure.core.async :as async :refer [<! >! chan go pipe pipeline]]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [loom.core :as loom]
            [cognitect.aws.api :as aws]))

(defn list-objects-pipeline [{:keys [client bucket prefix]}]
  (let [list-objects-request {:op :ListObjects :request {:Bucket bucket :Prefix prefix}}]
    (pipeline 1 (chan 1) (fn [response] (:Contents response)) (aws/invoke-async client list-objects-request))))

(defn get-object-pipeline [{:keys [client bucket files-channel output-channel error-channel]}]
  (pipeline 1 output-channel (fn [file]
                               (let [get-object-request {:op :GetObject :request {:Bucket bucket :Key (:Key file)}}]
                                 (aws/invoke-async client get-object-request)))
            (fn [input-stream]
              (if (:error input-stream)
                (go (>! error-channel (:error input-stream)))
                input-stream))
            files-channel))




(defn stream-files-from-s3-bucket [{:keys [client bucket prefix xform-provider params]}]
  (let [error-channel (chan 1)
        files-channel (list-objects-pipeline {:client client :bucket bucket :prefix prefix})
        output-channel (chan 1)
        get-object-opts {:client client :bucket bucket :files-channel files-channel :output-channel output-channel :error-channel error-channel}
        transducer (xform-provider params)]

    (pipe (pipeline 1 output-channel transducer (get-object-pipeline get-object-opts))
          (clojure.core.async/to-chan (io/writer *out*)))
    (go (while true
          (let [error (<! error-channel)]
            (println (json/write-str (airbyte/wrap-log error))))))))

(comment

  (defn topology-builder
    "Takes topic metadata and returns a function that builds the topology."
    [topic-metadata]
    (let [entities {:chan/list-objects-pipeline (assoc (:list-objects-pipeline topic-metadata) :etlp/entity-type :channel)
                    :chan/get-object-pipeleine (assoc (:get-object-pipeline topic-metadata) :etlp/entity-type :channel)
                    :chan/transform-message {:etlp/entity-type :channel
                                             :etlp/xform (comp
                                                          (map airbyte/wrap-record)
                                                          (map json/write-str))}}
        ; We are good with this simple flow for now we can move data between clojure channels via transducers 
        ; through a DAG like workflow
          workflow   [[:chan/list-objects-pipeline :chan/get-object-pipeleine]
                      [:chan/get-object-pipeleine :chan/transform-message]]]

      {:workflow workflow
       :entities entities}))


  (def mock-topology {:entities {:chan/list-objects-pipeline {:eltp/chan-provider list-objects-pipeline :etlp/entity-type :channel}
                                 :chan/get-object-pipeleine {:eltp/chan-provider get-object-pipeline :etlp/entity-type :channel}
                                 :chan/transform-message {:etlp/entity-type :output
                                                          :etlp/xform-provider (fn [params]
                                                                                 (comp
                                                                                  (map airbyte/wrap-record)
                                                                                  (map json/write-str)))}}
                      :workflow [[:chan/list-objects-pipeline :chan/get-object-pipeleine]
                                 [:chan/get-object-pipeleine :chan/transform-message]]})

  (defn build-dag [topology]
    (let [entities (:entities topology)
          workflow (:workflow topology)
          graph (loom/dag)]

      (doseq [edge workflow]
        (let [[from-entity to-entity] edge]
          (loom/add-edge graph (entities from-entity) (entities to-entity))))
      graph))

  (def my-dag (build-dag mock-topology))

(defn traverse-dag [dag]
  (let [entities (:entities dag)
        workflow (:workflow dag)
        channels (atom {})
        error-channel (chan 1)]
    (doseq [edge workflow]
      (let [[from-node to-node] edge
            from-channel (get @channels from-node)
            to-channel (get @channels to-node)
            xform-fn (:etlp/xform (get entities to-node))]
        (if (nil? from-channel)
          (let [from-fn (:etlp/fn (get entities from-node))
                from-channel (from-fn {})]
            (swap! channels assoc from-node from-channel)))
        (if (nil? to-channel)
          (let [to-channel (chan 1)]
            (swap! channels assoc to-node to-channel)))
        (async/pipe (async/pipeline 1 to-channel xform-fn (filter #(not (:error %)) from-channel))
                    (async/to-chan (async/go (while true
                                               (let [error (<! error-channel)]
                                                 (println (json/write-str error)))))))))))
  (def core-executor (fn [spec]
                     ; Traverse the topoplogy for each entitiy in :entities, it would have a key of names like :list-object-pipeline etc which would be 
                     ; a function it should return a core.asycn channel based on the information in the map
                     ; Each provided function should take input as a Map 
                     ; analyizing the :workflow , it's a DAG representation, we can use something like clojure/loom to build this graph and 
                     ; based on the workflow marshall the data from starting node or channel to output channel 
                       (let [exec-fn (traverse-dag my-dag)]
                         (exec-fn spec)))))

