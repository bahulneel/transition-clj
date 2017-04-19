(ns transition.rule-test
  (:require [transition.rule :as rule]
            [clojure.test :as t]
            [clojure.spec.test :as stest]
            [datomic.api :as d])
  (:import (clojure.lang ExceptionInfo)))

(stest/instrument)

(defn db-with
  [db tx]
  (:db-after (d/with db tx)))

(defn db-with-txes
  [db txes]
  (reduce db-with db txes))

(defn empty-db
  [name schema]
  (let [uri (str "datomic:mem://" name)]
    (d/delete-database uri)
    (d/create-database uri)
    (-> (d/db (d/connect uri))
        (db-with-txes rule/schema-txes)
        (db-with schema))))

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
  (t/testing "Unification"
    (let [db (empty-db ::db schema)]
      (t/is (= [:match {:arg2 :v2}]
               (first (second (rule/fire '#:transition.rule {:action [:event {:arg1 :v1 :arg2 ?v2}]
                                                             :event [:match {:arg2 ?v2}]}
                                         db
                                         [:event {:arg1 :v1 :arg2 :v2}]
                                         {})))))
      (t/is (= [:match {:arg2 :v2}]
               (first (second (rule/fire '#:transition.rule {:action [:event {:arg2 ?v2 :arg1 :v1}]
                                                             :event [:match {:arg2 ?v2}]}
                                         db
                                         [:event {:arg1 :v1 :arg2 :v2}]
                                         {})))))
      (t/is (= [:match {:arg2 :v2}]
               (first (second (rule/fire '#:transition.rule {:action [:event {:arg1 :v1 :arg2 ?v2}]
                                                             :event [:match {:arg2 ?v2}]}
                                         db
                                         [:event {:arg2 :v2 :arg1 :v1}]
                                         {})))))))

  (t/testing "firing a matching rule with a met precondition"
    (let [db (empty-db ::db schema)
          name "Ford Prefect"
          id (d/squuid)
          event [::create-customer #:customer {:name name}]
          [tx events] (with-redefs [datomic.api/squuid (constantly id)]
                        (rule/fire create-customer db event {}))
          db' (db-with db tx)]
      (t/is (= [[:transition.rule/applicable?
                 (get create-customer :transition.rule/precondition)
                 '{?name "Ford Prefect"}]
                #:customer {:name name :id id}]
               tx))
      (t/is (= [[::customer-created #:customer {:name name :id id}]]
               events))))

  (t/testing "firing a matching rule with a failed precondiiton"
    (let [id (d/squuid)
          name "Ford Prefect"
          db (db-with (empty-db ::db schema)
                      [#:customer {:name name :id id}])
          event [::create-customer #:customer {:name name}]
          res (rule/fire create-customer db event {})]
      (t/is (nil? res))))

  (t/testing "firing a rule then transacting twice"
    (let [db (empty-db ::db schema)
          name "Ford Prefect"
          id (d/squuid)
          event [::create-customer #:customer {:name name}]
          [tx events] (with-redefs [datomic.api/squuid (constantly id)]
                        (rule/fire create-customer db event {}))
          db' (db-with db tx)]
      (t/is (thrown-with-msg? ExceptionInfo
                              #"TX no longer applicable"
                              (db-with db' tx))))))
