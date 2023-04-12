(ns etlp.core
  (:require [clojure.string :as s]
            [clojure.tools.logging :refer [debug info]]
            [etlp.stream :as es]
            [etlp.connection :as ec]
            [integrant.core :as ig]
            [etlp.connection :as ec])
  (:gen-class))

(def *etl-config (atom nil))

(def *etlp-app (atom nil))

(defmulti etlp-component
  "Multi method to add extenstions to etlp"
  (fn [x]
    (get x :component)))

(defmethod etlp-component ::processors [{:keys [id component ctx]}]
  (let [plugin {:process-fn (or (get ctx :process-fn) nil)
                :etlp-config (or (get ctx :etlp-config) nil)
                :etlp-mapper (or (get ctx :etlp-mapper) {})}]

    (swap! *etl-config assoc-in [::processors (:name ctx)] plugin)))


(defmethod etlp-component :default [params]
  (throw (IllegalArgumentException.
          (str "I don't know the " (get params :component) " support"))))

(defn ig-wrap-schema [params]
  (fn []
    (if-let [shape @*etl-config]
      shape
      (reset! *etl-config   {::processors {}}))))

(def schema (ig-wrap-schema {}))

(defmulti invoke-connector (fn [ctx]
                             (get ctx :exec)))

(defmethod invoke-connector ::start [{:keys [ connector options]}]
  (println "Should invoke with :: " options)
  (ec/start connector))

(defmethod invoke-connector ::stop [{:keys [ connector options]}]
  (println "Should stop with :: " options)
  (ec/stop connector))

(defmethod invoke-connector :default [params]
  (throw (IllegalArgumentException.
          (str "Operation " (get params :exec) " not supported"))))


(defn exec-processor
  "run etlp processor" [ctx {:keys [processor params]}]
  (let [executor (get-in ctx [:etlp.core/processors processor])]
    (invoke-connector {:exec (params :command) :connector executor :options params})))

(defn init [{:keys [components] :as params}]
  (schema)
  (loop [x (dec (count components))]
    (when (>= x 0)
      (etlp-component (nth components x))
      (recur (dec x))))

  (defmethod ig/init-key ::processors [_ processors]
    (reduce-kv (fn [acc k ctx]
                 (assoc acc k (es/create-etlp-processor ctx)))
               {} processors))

  (reset! *etlp-app (ig/init (schema)))
  (partial exec-processor @*etlp-app))
