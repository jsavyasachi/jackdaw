(ns jackdaw.client.partitioning-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [jackdaw.client :as client]
   [jackdaw.client.partitioning :as part]
   [jackdaw.utils :as utils]))

(set! *warn-on-reflection* false)

(deftest test-record-key->key-fn
  (let [test-key-fn (fn [key-str]
                      (-> (part/record-key->key-fn {:record-key key-str})
                          :jackdaw.client.partitioning/key-fn))]

    (testing "dollar prefix"
      (is (= 42 ((test-key-fn "$.foo") {:foo 42}))))

    (testing "hyphenated"
      (is (= 42 ((test-key-fn "foo_bar") {:foo-bar 42}))))

    (testing "dotted"
      (is (= 42 ((test-key-fn "foo.bar") {:foo {:bar 42}}))))))


(deftest test->ProducerRecord
  (with-open [p (client/producer {"bootstrap.servers" (utils/bootstrap-servers)
                                  "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
                                  "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"})]
    (testing "absent key-fn"
      (let [record (part/->ProducerRecord p {:topic-name "foo"} "yolo")]
        (is (= "yolo" (.value record)))))

    (testing "identity key-fn"
      (let [record (part/->ProducerRecord p {:topic-name "foo"
                                             :key-fn identity} "yolo")]
        (is (= "yolo" (.value record)))
        (is (= "yolo" (.key record)))))

    (testing "explicit key"
      (let [record (part/->ProducerRecord p {:topic-name "foo"} "42" "yolo")]
        (is (= "yolo" (.value record)))
        (is (= "42" (.key record)))))

    (testing "explicit partition"
      (let [record (part/->ProducerRecord p {:topic-name "foo"} 1 "42" "yolo")]
        (is (= "yolo" (.value record)))
        (is (= "42" (.key record)))
        (is (= 1 (.partition record)))))

    (testing "explicit timestamp"
      (let [record (part/->ProducerRecord p {:topic-name "foo"} 1 0 "42" "yolo")]
        (is (= "yolo" (.value record)))
        (is (= "42" (.key record)))
        (is (= 1 (.partition record)))
        (is (= 0 (.timestamp record)))))))

    ;; TODO: How do callers inject headers?

(deftest produce!-with-key-preserves-caller-key
  (let [sent-record (atom nil)
        topic {:topic-name "foo"}
        key "caller-key"]
    (with-redefs [client/send! (fn [_producer record]
                                 (reset! sent-record record))]
      (part/produce! nil topic key "value"))
    (is (= key (.key @sent-record)))))

(deftest produce!-with-partition-preserves-caller-key-and-partition
  (let [sent-record (atom nil)
        topic {:topic-name "foo"}
        partition 2
        key "caller-key"]
    (with-redefs [client/send! (fn [_producer record]
                                 (reset! sent-record record))]
      (part/produce! nil topic partition key "value"))
    (is (= key (.key @sent-record)))
    (is (= partition (.partition @sent-record)))))

(deftest produce!-with-partition-and-timestamp-preserves-caller-key
  (let [sent-record (atom nil)
        topic {:topic-name "foo"}
        partition 2
        timestamp 42
        key "caller-key"]
    (with-redefs [client/send! (fn [_producer record]
                                 (reset! sent-record record))]
      (part/produce! nil topic partition timestamp key "value"))
    (is (= key (.key @sent-record)))
    (is (= partition (.partition @sent-record)))
    (is (= timestamp (.timestamp @sent-record)))))

(deftest produce!-with-headers-preserves-caller-key
  (let [sent-record (atom nil)
        topic {:topic-name "foo"}
        partition 2
        timestamp 42
        key "caller-key"
        headers (org.apache.kafka.common.header.internals.RecordHeaders.)]
    (with-redefs [client/send! (fn [_producer record]
                                 (reset! sent-record record))]
      (part/produce! nil topic partition timestamp key "value" headers))
    (is (= key (.key @sent-record)))
    (is (= partition (.partition @sent-record)))
    (is (= timestamp (.timestamp @sent-record)))))
