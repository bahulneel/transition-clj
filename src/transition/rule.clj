(ns transition.rule
  (:require [clojure.spec :as s]
            [lab79.datomic-spec]
            [datomic.api :as d]
            [clojure.core.unify :as u]
            [clojure.walk :as walk]))

(s/def ::definition
  (s/keys :req [::action (or ::event ::effect)]
          :opt [::precondition ::context ::aggregate]))

(s/def ::event
  (s/tuple keyword? ::args))

(s/def ::action
  (s/tuple keyword? ::args))

(s/def ::args
  (s/map-of keyword? (s/or :variable :datalog/variable
                           :arg :datomic-spec.value/any)))

(s/def ::aggregate
  (s/map-of symbol? any?))

(s/def ::effect
  (s/coll-of :datalog/data-pattern :min-size 1))

(s/def ::context
  :datomic.query.kv/where)

(s/def ::precondition
  :datomic.query.kv/where)

(s/def ::ground-args
  (s/map-of keyword? any?))

(s/def ::ground-context
  ::ground-args)

(s/def ::ground-event
  (s/tuple keyword? ::ground-args))

(s/fdef fire
        :args (s/cat :rule ::definition
                     :db any?
                     :event ::ground-event
                     :consts ::ground-args))

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

(defn keyword>lvar
  [k]
  (->> k
       name
       (str "?")
       symbol))

(defn unify-event
  [event db args context consts alias]
  (let [find (u/subst (vec (distinct (into (lvars event) (keys consts))))
                      alias)
        const-vars (vec (keys consts))
        arg-vars (vec (keys args))
        in (vector '$ [const-vars] [arg-vars])
        q {:find find :in in :where context}]
    (if (seq find)
      (do
        #_(prn q)
        (let [b (seq (map (fn [match]
                            (reduce-kv (fn [b var alias]
                                         (assoc b var (get b alias)))
                                       (zipmap find match)
                                       alias))
                          (d/q q db [(vals consts)] [(vals args)])))]
          #_(prn b)
          b))
      [{}])))

(def schema-txes
  '[[{:db/ident ::applicable?
      :db/fn #db/fn {:lang   "clojure"
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
                                                  :data         data}))))}}]])

(defn normalise
  [event]
  (walk/postwalk (fn [x]
                   (if (map? x)
                     (into (sorted-map) x)
                     x))
                 event))

(defn fire
  [rule db ground-event consts]
  (let [{:keys [::event
                ::aggregate
                ::action
                ::effect
                ::context
                ::precondition]} rule
        ground-event (normalise ground-event)
        action (normalise action)
        const-lvars (into {}
                          (map (fn [[k v]]
                                 [(keyword>lvar k) v]))
                          consts)
        args (u/unify action ground-event)
        tx (and args (->> (unify-event action
                                       db
                                       args
                                       precondition
                                       const-lvars
                                       {})
                          (map #(merge args %))
                          (map #(vector ::applicable?
                                        precondition
                                        (merge const-lvars %)))
                          seq))]
    (when-let [matches (and tx (unify-event event
                                            db
                                            args
                                            context
                                            const-lvars
                                            (or aggregate {})))]
      (let [tx (into (vec tx) (mapcat #(u/subst effect (merge args %)) matches))
            events (map #(u/subst event (merge args %)) matches)]
        [tx events]))))
