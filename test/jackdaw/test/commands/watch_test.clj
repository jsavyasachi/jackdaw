(ns jackdaw.test.commands.watch-test
  (:require
   [jackdaw.test.commands.watch :as watch]
   [manifold.deferred :as d]
   [clojure.test :refer [deftest testing is]]))

(set! *warn-on-reflection* false)

;; This example shows the problem that the watcher solves.
;;
;; A process adds messages for orders. Each message has a pair (qty, amt). The
;; test watches the journal until it receives $100 of revenue. It then returns.
;;
;; The test sends the messages. It asserts that `handle-cmd!` returns only after
;; it receives messages for $100 of revenue.

(defn run-watch-cmd [watcher cmd-list]
  (let [journal (agent [])
        machine {:journal journal}
        result-d (d/future (watch/handle-watch-cmd machine [watcher]))]

    (doseq [cmd (butlast cmd-list)]
      (send journal conj cmd)
      (is (not (d/realized? result-d))))

    (send journal conj (last cmd-list))

    @result-d))

(deftest test-watch-command
  (testing "watch for $100 revenue"
    (let [rev (fn [coll]
                (reduce + 0 (map (fn [[qty amt]]
                                   (* qty amt))
                                 coll)))]
      (run-watch-cmd (fn [j]
                       (<= 100 (rev j)))
                     [[2 10]
                      [5 10]
                      [3 10]
                      [1 10]]))))
