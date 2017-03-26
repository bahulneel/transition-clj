(ns transition.rule
  (:require [clojure.spec :as s]
            [lab79.datomic-spec]
            [datomic.api :as d]
            [clojure.core.unify :as u]))

(s/def ::definition
  (s/keys :req [::event ::action ::effect]
          :opt [::precondition ::context]))

(s/def ::event
  (s/tuple keyword? ::args))

(s/def ::action
  (s/tuple keyword? ::args))

(s/def ::args
  (s/map-of keyword? :datalog/variable))

(s/def ::effect
  (s/coll-of :datalog/data-pattern :min-size 1))

(s/def ::context
  :datomic.query.kv/where)

(s/def ::precondition
  :datomic.query.kv/where)

(s/def ::ground-args
  (s/map-of keyword? :datomic-spec.value/any))

(s/def ::ground-context
  ::ground-args)

(s/def ::ground-event
  (s/tuple keyword? ::ground-args))

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

(defn unify-event
  [event db args context]
  (let [find (vec (lvars event))
        in (vector '$ (vec (keys args)))
        q {:find find :in in :where context}]
    (seq (map (fn [match]
                (zipmap find match))
              (d/q q db (vals args))))))

(def schema
  '[{:db/ident ::applicable?
     :db/fn    #db/fn {:lang   "clojure"
                       :params [db precondition data]
                       :code   (let [ks (vec (keys data))
                                     vs (vec (vals data))
                                     q {:find  ks
                                        :in    ['$ ks]
                                        :where precondition}
                                     applicable? (seq (datomic.api/q q db vs))]
                                 (when-not applicable?
                                   (throw (ex-info "TX no longer applicable"
                                                   {:precondition precondition
                                                    :data         data}))))}}])

(defn fire
  [rule db ground-event]
  (let [{:keys [::event ::action ::effect ::context ::precondition]} rule
        args (u/unify action ground-event)
        tx (and args (->> (unify-event action db args precondition)
                          (map #(merge args %))
                          (map #(vector ::applicable? precondition %))
                          seq))]
    (when-let [matches (and tx (unify-event event db args context))]
      (let [tx (into (vec tx) (mapcat #(u/subst effect %) matches))
            events (map #(u/subst event %) matches)]
        [tx events]))))
