(ns transition.util
  (:require [clojure.java.io :as io])
  (:import [datomic Util]))

(defn read-dtm
  "Reads a dtm file (i.e., an edn file with datomic tags in it) from the classpath
  and returns a vector of all forms contained within."
  [filename]
  (-> (io/resource filename) (io/reader) (Util/readAll)))
