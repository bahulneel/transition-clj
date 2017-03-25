(ns transition.rule
  (:require [clojure.spec :as s]
            [lab79.datomic-spec]
            [datomic.api :as d]
            [clojure.core.unify :as u]))

(s/def ::definition
  (s/keys :req [::event ::on ::effect]
          :opt [::where]))

(s/def ::event
  (s/tuple keyword? ::args))

(s/def ::on
  (s/or :ea (s/tuple keyword? ::args)
        :eac (s/tuple keyword? ::args ::context)))

(s/def ::args
  (s/map-of keyword? :datalog/variable))

(s/def ::context
  ::args)

(s/def ::effect
  (s/coll-of :datalog/data-pattern :min-size 1))

(s/def ::where
  :datomic.query.kv/where)

(s/def ::ground-args
  (s/map-of keyword? :datomic-spec.value/any))

(s/def ::ground-context
  ::ground-args)

(s/def ::ground-event
  (s/or :ea (s/tuple keyword? ::ground-args)
        :eac (s/tuple keyword? ::ground-args ::ground-context)))

(s/fdef fire
        :args (s/cat :rule ::definition
                     :db any?
                     :event ::ground-event))

(defn lvar?
  [x]
  (and (symbol? x)
       (nil? (namespace x))
       (= \? (first (name x)))))

(defn lvars
  [x]
  (cond
    (sequential? x) (distinct (mapcat lvars x))
    (map? x) (distinct (lvars (vals x)))
    (lvar? x) [x]
    :else nil))

(defn fire
  [rule db ground-event]
  (let [{:keys [::event ::on ::effect ::where]} rule]
    (when-let [args (u/unify on ground-event)]
      (let [find (vec (lvars event))
            in (vector '$ (vec (keys args)))
            q {:find find :in in :where where}]
        (when-let [matches (seq (map (fn [match]
                                       (zipmap find match))
                                     (d/q q db (vals args))))]
          (let [tx (mapcat #(u/subst effect %) matches)
                events (map #(u/subst event %) matches)]
            [tx events]))))))
