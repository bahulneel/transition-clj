(defproject transition "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0-alpha14" :scope "provided"]
                 [org.clojure/core.async "0.3.442"]
                 [lab79/datomic-spec "0.2.0-alpha6"]
                 [org.clojure/core.unify "0.5.7"]
                 [com.datomic/datomic-free "0.9.5561" :scope "test"]]
  :cljsbuild {:builds []})
