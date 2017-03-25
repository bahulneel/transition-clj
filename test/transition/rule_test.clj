(ns transition.rule-test
  (:require [transition.rule :as rule]
            [clojure.test :as t]
            [clojure.spec.test :as stest]
            [datomic.api :as d]))

(stest/instrument)

(defn db-with
  [db tx]
  (:db-after (d/with db tx)))

(defn empty-db
  [name schema]
  (let [uri (str "datomic:mem://" name)]
    (d/delete-database uri)
    (d/create-database uri)
    (db-with (d/db (d/connect uri)) schema)))

(def schema
  [{:db/ident       :customer/name
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident       :customer/id
    :db/valueType   :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity}])

(def create-customer
  '#:transition.rule
      {:action       [::create-customer #:customer {:name ?name}]
       :precondition [(not [_ :customer/name ?name])]
       :context      [[(datomic.api/squuid) ?id]]
       :effect       [#:customer {:name ?name :id ?id}]
       :event        [::customer-created #:customer {:name ?name :id ?id}]})

(t/deftest rule-firing
  (t/testing "firing a matching rule with no conflicts"
    (let [db (empty-db ::db schema)
          name "Ford Prefect"
          id (d/squuid)
          event [::create-customer #:customer {:name name}]
          [tx events] (with-redefs [datomic.api/squuid (constantly id)]
                        (rule/fire create-customer db event))]
      (t/is (= [#:customer {:name name :id id}]
               tx))
      (t/is (= [[::customer-created #:customer {:name name :id id}]]
               events))))

  (t/testing "firing a matching rule with a failed precondiiton"
    (let [id (d/squuid)
          name "Ford Prefect"
          db (db-with (empty-db ::db schema)
                      [#:customer {:name name :id id}])
          event [::create-customer #:customer {:name name}]
          [tx events] (rule/fire create-customer db event)]
      (t/is (empty? tx))
      (t/is (empty? events)))))
