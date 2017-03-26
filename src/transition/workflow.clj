(ns transition.workflow
  (:require [clojure.spec :as s]
            [clojure.core.async :as a]))

(defmulti step-type first)

(defmethod step-type :sync
  [_]
  ::pipeline)

(defmethod step-type :async
  [_]
  ::pipeline)

(defmethod step-type :blocking
  [_]
  ::pipeline)

(defmethod step-type :merge
  [_]
  (s/tuple #{:merge} (s/coll-of keyword?)))

(s/def ::pipeline
  (s/tuple keyword? keyword? fn?))

(s/def ::step
  (s/multi-spec step-type (fn [v t]
                            (into [t] v))))

(s/def ::workflow
  (s/map-of keyword? ::step))

(s/def ::source
  any?)

(s/def ::sources
  (s/map-of keyword? ::source))

(s/fdef init-sources
        :args (s/cat :source keyword?)
        :ret ::sources)

(s/fdef build-workflow
        :args (s/cat :workflow ::workflow
                     :sources ::sources)
        :ret ::sources)

(s/fdef stop-workflow!
        :args (s/cat :sources ::sources))

(s/fdef step-applicable?
        :args (s/cat :sources ::sources
                     :step (s/tuple keyword? ::step))
        :ret boolean?)

(s/fdef add-step
        :args (s/cat :sources ::sources
                     :step (s/tuple keyword? ::step)))

(defn step-applicable?
  [sources [id step]]
  (let [input (second step)
        applicable? (partial contains? sources)]
    (if (coll? input)
      (every? applicable? input)
      (applicable? input))))

(def build-step nil)

(defmulti build-step (fn [_ [id [type]]]
                       type))

(defn add-step
  [sources [id step]]
  (let [new-source (build-step sources step)]
    (assoc sources id new-source)))

(defn init-sources
  [init-source]
  {init-source (a/chan)})

(defn build-workflow
  [w sources]
  (loop [sources sources
         steps (seq w)]
    (if-let [ready-steps (filter #(step-applicable? sources %)
                                 steps)]
      (let [next-sources (reduce add-step sources ready-steps)
            next-steps (remove #(step-applicable? sources %)
                               steps)]
        (recur next-sources next-steps))
      sources)))

(defn stop-workflow!
  [sources]
  (doseq [[id c] sources]
    (a/close! c)))

(defmethod build-step :sync
  [sources [_ source xf]]
  (let [source-c (get sources source)
        sink-c (a/chan 1)]
    (a/pipeline 1 sink-c xf source-c)
    sink-c))

(defmethod build-step :async
  [sources [_ source xf]]
  (let [source-c (get sources source)
        sink-c (a/chan 1)]
    (a/pipeline-async 1 sink-c xf source-c)
    sink-c))

(defmethod build-step :blocking
  [sources [_ source xf]]
  (let [source-c (get sources source)
        sink-c (a/chan 1)]
    (a/pipeline-blocking 1 sink-c xf source-c)
    sink-c))

(defmethod build-step :merge
  [sources [_ inputs]]
  (a/merge inputs))
