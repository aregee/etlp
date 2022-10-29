(ns etlp.core
  (:require [clojure.tools.logging :refer [debug info]]
            [etlp.stream :as es]
            [etlp.kstream :as ek]
            [etlp.reducers :as reducers]
            [integrant.core :as ig])
  (:gen-class))

(def *etl-config (atom nil))

(def *etlp-app (atom nil))

(def build-message-topic es/build-message-topic)

(def lagging-transducer es/build-lagging-transducer)

(defn create-kstream-topology-processor [{:keys [config topic-metadata topology-builder topic-reducers] :as ctx}]
  (fn [opts]
    (ek/kafka-stream-topology-processor (merge ctx {:params opts}))))

(defn create-kafka-stream-processor [ctx]
  (fn [opts]
    (es/directory-to-kafka-stream-processor (merge ctx {:params opts}))))

(defn create-pg-stream-processor [ctx]
  (fn [opts]
    (es/directory-to-db-stream-processor (merge ctx {:params opts}))))

(defn logger [line]
  (debug line)
  line)

(defn custom-file-reducer [xform-provider]
  (fn [opts]
    (fn [filepath]
      (info filepath)
      (eduction
       (xform-provider filepath opts)
       (reducers/read-lines filepath)))))

(defmulti etlp-component
  "Multi method to add extenstions to etlp"
  (fn [x]
    (get x :component)))

(defmethod etlp-component ::processors [{:keys [id component ctx]}]
  (let [plugin {:run (or (get ctx :process-fn) nil)
                :type (or (get ctx :type) nil)
                :config (ig/ref ::config)
                :table-opts (or (get ctx :table-opts) nil)
                :topic (or (get ctx :topic) nil)
                :topic-metadata (or (get ctx :topic-metadata) nil)
                :topic-reducers (or (get ctx :topology-reducers) nil)
                :topology-builder (or (get ctx :topology-builder) nil)
                :reducer (or (get ctx :reducer) nil)
                :xform-provider (or (get ctx :xform-provider) nil)
                :reducers (ig/ref ::reducers)
                :default-processors (ig/ref ::default-processors)}]
    (swap! *etl-config assoc-in [::processors (:name ctx)] plugin)))

(defmethod etlp-component ::reducers [{:keys [id component ctx]}]
  (let [plugin (custom-file-reducer (:xform-provider ctx))]
    (swap! *etl-config assoc-in [::reducers (:name ctx)] plugin)))

(defmethod etlp-component ::config [{:keys [id component ctx]}]
  (let [plugin ctx]
    (swap! *etl-config assoc-in [::config (:name plugin)] (dissoc plugin :name))))

(defmethod etlp-component :default [params]
  (throw (IllegalArgumentException.
          (str "I don't know the " (get params :component) " support"))))

(defn ig-wrap-schema [params]
  (fn []
    (if-let [shape @*etl-config]
      shape
      (reset! *etl-config   {::config {:db nil
                                       :kafka nil}
                             ::reducers {:directory-reducer reducers/parallel-directory-reducer
                                         :file-reducer reducers/file-reducer}
                             ::default-processors {}
                             ::processors {}}))))

(def schema (ig-wrap-schema {}))

(def etlp-json-reducer {:id 1
                        :component ::reducers
                        :ctx {:name :json-reducer
                              :xform-provider (fn [filepath opts]
                                                (map (reducers/parse-line filepath opts)))}})
(def etlp-line-reducer {:id 1
                        :component ::reducers
                        :ctx {:name :line-reducer
                              :xform-provider (fn [filepath opts]
                                                (map logger))}})
(defn exec-processor
  "run etlp processor" [ctx {:keys [processor params]}]
  (let [executor (get-in ctx [:etlp.core/processors processor])]
    (executor params)))


(defn init [{:keys [components] :as params}]
  (schema)
  (etlp-component etlp-json-reducer)
  (etlp-component etlp-line-reducer)
  (loop [x (dec (count components))]
    (when (>= x 0)
      (etlp-component (nth components x))
      (recur (dec x))))


  (defmethod ig/init-key ::config [_ {:keys [db kafka] :as opts}] opts)

  (defmethod ig/init-key ::reducers [_ ctx]
    ctx)

  (defmethod ig/init-key ::default-processors [_ ctx]
    ctx)

  (defmethod ig/init-key ::processors [_ processors]
    (reduce-kv (fn [acc k ctx]
                 (if (not (nil? (:type ctx)))
                   (let [process-fn (get-in ctx [:default-processors (:type ctx) :run])
                         reducer (get-in ctx [:default-processors (:type ctx) :reducer])]
                     (assoc acc k (process-fn (merge ctx {:reducer reducer}))))
                   (assoc acc k ((:run ctx) ctx)))) {} processors))

  (reset! *etlp-app (ig/init (schema)))
  (partial exec-processor @*etlp-app))
