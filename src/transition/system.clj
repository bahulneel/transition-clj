(ns transition.system
  (:require [transition.rule :as rule]
            [transition.workflow :as wf]
            [transition.event :as event]
            [datomic.api :as d]
            [clojure.spec :as s]))

(s/def ::conn any?)

(s/def ::rule
  ::rule/definition)

(s/def ::event
  ::event/definition)

(s/def ::events
  (s/coll-of ::event))

(s/def ::rules
  (s/coll-of ::rule :kind set?))

(s/def ::workflow
  ::wf/sources)

(s/def ::system
  (s/keys :req [::conn ::rules]
          :opt [::workflow]))

(s/def ::trigger
  (s/keys :req [::conn ::event ::context ::rule]))

(s/def ::effect
  (s/keys :req [::trigger ::tx ::events]))

(s/def ::success
  (s/keys :req [::context ::events ::tx-report]))

(s/def ::failure
  (s/keys :req [::trigger ::error]
          :opt [::tx]))

(s/def ::context
  (s/keys :req [::max-attempts]
          :opt [::attempt ::tx-meta]))

(s/fdef system
        :args (s/cat :conn ::conn)
        :ret ::system)

(s/fdef add-rule
        :args (s/cat :system ::system
                     :rule ::rule)
        :ret ::system)

(s/fdef trigger
        :args (s/cat :system ::system
                     :event (s/tuple ::event ::context))
        :ret (s/coll-of ::trigger))

(s/fdef fire
        :args (s/cat :trigger ::trigger)
        :ret ::effect)

(s/fdef affect
        :args (s/cat :effect ::effect)
        :ret (s/or :success ::success
                   :failure ::failure))

(def schema-txes
  [[{:db/ident       ::cause
     :db/valueType   :db.type/ref
     :db/cardinality :db.cardinality/one
     :db/doc         "Event that cause the transition"}
    {:db/ident       ::effect
     :db/valueType   :db.type/ref
     :db/cardinality :db.cardinality/many
     :db/doc         "Side effect of the transition"}]])

(defn system
  [conn]
  {::conn  conn
   ::rules #{}})

(defn add-rule
  [system rule]
  (update system ::rules conj rule))

(defn trigger
  [system [event context]]
  (println "trigger" event context)
  (let [{:keys [::conn ::rules]} system]
    (map (fn [rule]
           {::conn    conn
            ::event   event
            ::context context
            ::rule    rule})
         rules)))

(defn fire
  [trigger]
  #_(println "fire" trigger)
  (let [{:keys [::conn ::event ::rule ::context]} trigger
        {:keys [::tx-meta]} context
        db (d/db conn)
        [tx events] (rule/fire rule db (::event/event event) tx-meta)
        trigger' (update-in trigger [::context ::attempt] (fnil inc 0))]
    (when (or (seq tx) (seq events))
      {::trigger trigger'
       ::tx      tx
       ::events  (mapv #(event/event % event) events)})))

(defn affect
  [effect]
  (println "affect")
  #_(clojure.pprint/pprint effect)
  (let [{:keys [::trigger ::tx ::events]} effect
        {:keys [::conn ::context ::event]} trigger
        {:keys [::tx-meta]} context
        event-tx (map event/event>entity (into (if event
                                                 [event]
                                                 [])
                                               events))
        tx-meta (cond-> {:db/id "datomic-tx"}
                  tx-meta (merge tx-meta)
                  event (assoc ::cause (str (::event/id event)))
                  (seq events) (assoc ::effect
                                      (mapv (comp str ::event/id)
                                            events)))

        full-tx (->> event-tx
                     (into [tx-meta])
                     (into tx))
        tx-report (d/transact conn full-tx)]
    (try
      {::context   context
       ::events    events
       ::tx-report @tx-report}
      (catch Exception e
        {::trigger trigger
         ::error   e
         ::tx full-tx}))))

(defn success?
  [x]
  (s/valid? ::success x))

(defn failure?
  [x]
  (s/valid? ::failure x))

(defn retry?
  [failure]
  (when (failure? failure)
    (let [{:keys [::trigger]} failure
          {:keys [::context]} trigger
          {:keys [::attempt ::max-attempts]} context]
      (< attempt max-attempts))))

(defn workflow
  [system]
  {::trigger  [:sync ::event (mapcat (partial trigger system))]
;;   ::work [:merge [::retry? ::trigger]]
   ::fire     [:sync ::trigger (keep fire)]
   ::affect   [:blocking ::fire (map affect)]
   ::success? [:sync ::affect (filter success?)]
   ::failure? [:sync ::affect (remove success?)]
   ::retry?   [:sync ::failure? (filter retry?)]
   ::error?   [:sync ::failure? (remove retry?)]
;;   ::out      [:merge [::success? ::error?]]
   })

(defn start-system
  [system]
  (let [sources (wf/init-sources ::event)
        workflow (-> system
                     workflow
                     (wf/build-workflow sources))]
    (assoc system ::workflow workflow)))

(defn stop-system!
  [system]
  (if-let [workflow (::workflow system)]
    (do
      (wf/stop-workflow! workflow)
      (dissoc system ::workflow))
    system))

(defn schema
  []
  (->> schema-txes
       (into event/schema-txes)
       (into rule/schema-txes)))
