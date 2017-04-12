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

(defmulti build-step (fn [_ [type]]
                       type))

(defn add-step
  [sources [id step]]
  (let [new-source (build-step sources step)]
    (assoc sources id new-source)))

(defn init-sources
  [init-source]
  (let [source (a/chan)]
    {init-source [source (a/mult source)]}))

(defn build-workflow
  [w sources]
  (loop [sources sources
         steps (seq w)]
    (if-let [ready-steps (seq (filter #(step-applicable? sources %)
                                      steps))]
      (let [next-sources (reduce add-step sources ready-steps)
            next-steps (remove #(step-applicable? sources %)
                               steps)]
        (recur next-sources next-steps))
      sources)))

(defn put!
  [sources source msg]
  (let [done (promise)
        source-c (get-in sources [source 0])]
    (if source-c
      (a/go (deliver done (a/>! source-c msg)))
      (deliver done (ex-info "Not a source"
                             {:source source
                              :sources (keys sources)})))
    done))

(defn take!
  [sources source]
  (let [msg (promise)
        source-m (get-in sources [source 1])]
    (if source-m
      (let [source-c (a/promise-chan)]
        (a/go
          (a/tap source-m source-c)
          (deliver msg (a/<! source-c))
          (a/untap source-m source-c)))
      (deliver msg (ex-info "Not a source"
                             {:source source
                              :sources (keys sources)})))
    msg))

(defn stop-workflow!
  [sources]
  (doseq [[id [c _]] sources]
    (a/close! c)))

(defn tap-source
  [sources source]
  (let [source-m (get-in sources [source 1])
        source-c (a/chan 1)]
    (a/tap source-m source-c)
    source-c))

(defmethod build-step :sync
  [sources [_ source xf]]
  (let [source-c (tap-source sources source)
        sink-c (a/chan)
        sink-m (a/mult sink-c)]
    (a/pipeline 1 sink-c xf source-c)
    [sink-c sink-m]))

(defmethod build-step :async
  [sources [_ source xf]]
  (let [source-c (tap-source sources source)
        sink-c (a/chan)
        sink-m (a/mult sink-c)]
    (a/pipeline-async 1 sink-c xf source-c)
    [sink-c sink-m]))

(defmethod build-step :blocking
  [sources [_ source xf]]
  (let [source-c (tap-source sources source)
        sink-c (a/chan)
        sink-m (a/mult sink-c)]
    (a/pipeline-blocking 1 sink-c xf source-c)
    [sink-c sink-m]))

(defmethod build-step :merge
  [sources [_ inputs]]
  (let [source-cs (map #(tap-source sources %) inputs)
        sink-c (a/merge source-cs)
        sink-m (a/mult sink-c)]
    [sink-c sink-m]))
