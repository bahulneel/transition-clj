(ns transition.system
  (:require [transition.rule :as rule]
            [transition.workflow :as wf]
            [datomic.api :as d]
            [clojure.spec :as s]))

(s/def ::conn any?)

(s/def ::rule
  ::rule/definition)

(s/def ::event
  ::rule/ground-event)

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
  (s/keys :req [::trigger ::error]))

(s/def ::context
  (s/keys :req [::max-attempts]
          :opt [::attempt]))

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
(defn system
  [conn]
  {::conn  conn
   ::rules #{}})

(defn add-rule
  [system rule]
  (update system ::rules conj rule))

(defn trigger
  [system [event context]]
  (let [{:keys [::conn ::rules]} system]
    (map (fn [rule]
           {::conn   conn
            ::event  event
            ::contxt context
            ::rule   rule})
         rules)))

(defn fire
  [trigger]
  (let [{:keys [::conn ::event ::rule]} trigger
        db (d/db conn)
        [tx events] (rule/fire rule db event)
        trigger' (update trigger ::attempt (fnil inc 0))]
    {::trigger trigger'
     ::tx      tx
     ::events  events}))

(defn affect
  [effect]
  (let [{:keys [::trigger ::tx ::events]} effect
        {:keys [::conn ::context]} trigger
        tx-report (d/transact conn tx)]
    (try
      {::context   context
       ::events    ::events
       ::tx-report @tx-report}
      (catch Exception e
        {::trigger trigger
         ::error   e}))))

(defn success?
  [x]
  (s/valid? ::success x))

(defn failure?
  [x]
  (s/valid? ::failure x))

(defn retry?
  [failure]
  (when (failure? failure))
  (let [{:keys [::context]} failure
        {:keys [::attempt ::max-attempts]} context]
    (< attempt max-attempts)))

(defn workflow
  [system]
  {::trigger  [:sync ::event (mapcat (partial trigger system))]
   ::work     [:merge [::retry? ::trigger]]
   ::fire     [:sync (map fire)]
   ::affect   [:blocking ::fire (map affect)]
   ::success? [:sync ::affect (filter success?)]
   ::failure? [:sync ::affect (remove success?)]
   ::retry?   [:sync ::failure? (filter retry?)]
   ::error?   [:sync ::failure? (remove retry?)]
   ::out      [:merge [::success? ::error?]]})

(defn start-system
  [system]
  (let [sources (wf/init-sources ::event)
        workflow (->> system
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
