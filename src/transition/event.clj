(ns transition.event
  (:require [transition.rule :as rule]
            [datomic.api :as d]
            [clojure.spec :as s]))

(s/def ::id string?)

(s/def ::event
  ::rule/ground-event)

(s/def ::cause
  ::id)

(s/def ::definition
  (s/keys :req [::id ::event]
          :opt [::cause]))

(def schema
  [{:db/ident       ::id
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity
    :db/doc         "Unique event id"}
   {:db/ident       ::name
    :db/valueType   :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/index       true
    :db/doc         "Event name"}
   {:db/ident       ::args
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc         "Serialised (edn) event args"}
   {:db/ident       ::cause
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity
    :db/doc         "Cause of this event"}])

(defn event>entity
  [e]
  (let [{:keys [::id ::event ::cause]} e
        [n args] event]
    (cond-> {:db/id id
             ::id   id
             ::name n
             ::args (pr-str args)}
            cause (assoc ::cause cause))))

(defmulti event-id first)

(defmethod event-id :default
  [_]
  (str (d/squuid)))

(defn event
  ([e]
   (event e nil nil))
  ([e cause]
   (event e cause nil))
  ([e cause id]
   (cond-> {::event event ::id id}
           (nil? id) (assoc ::id (event-id e))
           (s/valid? ::definition cause) (assoc :cause (::id cause)))))

(defn id
  [event]
  (::id event))
