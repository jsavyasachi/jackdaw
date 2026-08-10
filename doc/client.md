# Jackdaw Client API

## Rationale

The Jackdaw Client API wraps the core Kafka `Producer`<sup>[1](#producerapi)</sup> and
`Consumer`<sup>[2](#consumerapi)</sup> APIs. It gives functions to build or
unpack the related objects, such as Callbacks, Serdes, and ConsumerRecords.

Kafka Streams, Kafka Connect, and KSQL all build on these core APIs. If you know
these core APIs well, you also know more about the related technologies.

Kafka's surface API is small, but its functions do much work. You can start
quickly with a simple example. To learn all of its capabilities, read the
upstream documentation. This guide shows how to use the API through Jackdaw. It
also points you to the related parts of the upstream documentation.

## Producing

The producer example below shows how to use the Kafka Producer API. The configuration<sup>[3](#producerconfig)</sup>
is a map. Jackdaw converts the map to a `Properties` object. This example
configures the producer with a small number of important options.

 * "bootstrap.servers=localhost:9092" tells the producer to make a connection to
   the kafka broker on the default port at localhost

 * "client.id=foo" puts the string 'foo' in all requests to brokers. The brokers
   can then identify a client by more than the host and the IP. The string is also
   part of the name of the metrics from the brokers and from the producer application

 * "acks=all" makes the leader wait for the full set of in-sync replicas to
   acknowledge the result and complete the response. This is the slowest setting,
   but it is the most durable. The default is '1'. With '1', the leader responds
   when it writes the record to its own log. This gives more throughput and less
   durability.

Usually you create producers with the `with-open` macro. The macro closes the
producer at the end of the body, or when the code throws an exception. By
default, the StringSerializer serializes the key and the value for the
ProducerRecord that goes to the leader.

In the body, the `jc/produce!` function requests a write to the given
Kafka topic. The function returns a delay immediately. `deref` the delay to wait
for the result of the Kafka `.send` call. The result includes metadata such as
the timestamp and the offset of the record.

The [KafkaProducer javadocs](https://kafka.apache.org/20/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html)
give more information about how the producer works.


```
(ns producer-example
  (:require
    [jackdaw.client :as jc]))

(def producer-config
  {"bootstrap.servers" "localhost:9092"
   "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
   "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"
   "acks" "all"
   "client.id" "foo"})

(with-open [my-producer (jc/producer producer-config)]
  @(jc/produce! my-producer {:topic-name "foo"} "1" "hi mom!"))
```

## Consuming

The consumer example below shows how to use the Kafka Consumer API. The configuration<sup>[5](#consumerconfig)</sup>
is a map. Jackdaw converts the map to a `Properties` object. This example
configures the Consumer with a small number of important options.

 * "bootstrap.servers=localhost:9092" tells the consumer to make a connection to
   the kafka broker on the default port at localhost

 * "group.id=foo" makes this consumer a part of the 'foo' consumer group. Other consumers
   with the same id make a pool of consumers. The pool shares the workload, and gives
   scalability and fault tolerance

Usually you create consumers with the `with-open` macro. The macro closes the
consumer at the end of the body, or when the code throws an exception. By default,
the StringDeserializer deserializes the key and the value for the ConsumerRecord.

First, create a consumer and subscribe it to a list of topics. Use the `jc/subscribed-consumer` function:
```
(with-open [consumer (jc/subscribed-consumer consumer-config [topic-config-1 topic-config-2 ...])
```
`subscribed-consumer` takes a `consumer-config` and a vector of `topic-configs`. It returns a `consumer` that subscribes to all of the given topics.

The main body of a poll loop for the consumer can look as follows:

```
(ns consumer-example
  (:require
    [jackdaw.client :as jc]))

(def consumer-config
  {"bootstrap.servers" "localhost:9092"
   "group.id" "com.foo.my-consumer"})

(def topic-config
  {:topic-name "foo"})

(defn poll-and-loop!
  "Continuously fetches records every `poll-ms`, processes them with `processing-fn` and commits offset after each poll."
  [consumer processing-fn continue?]
  (let [poll-ms 5000]
    (loop []
      (if @continue?
        (let [records (jc/poll consumer poll-ms)]
          (when (seq records)
            (processing-fn records)
            (.commitSync consumer))
          (recur))))))

(defn process-messages! [topic-config processing-fn]
  (let [continue? (atom true)]
    (with-open [consumer (jc/subscribed-consumer consumer-config [topic-config])]
      (poll-and-loop! consumer processing-fn continue?))))
```
This code creates a consumer and subscribes it to the "foo" topic. The `poll-and-loop` function fetches records every `poll-ms`. It processes them with `processing-fn` (specific to your application). It commits the offset after each poll. For a sample application that uses the Client API, see examples/rolldice<sup>[7](#clientapiexample)</sup>).

The `jackdaw.client.log/log` function is useful for tests. It takes a consumer instance that subscribes
to one or more topics, a poll interval in ms, and an optional `fuse-fn`. It returns a lazy infinite sequence of "datafied" records. The order is the order of the calls to the Consumer's `.poll` method. With a `fuse-fn`, the sequence stops after `fuse-fn` returns false. Without a `fuse-fn`, the function continues to poll. In this example, `jc/subscribe` makes the consumer see all records in the "foo"
topic. The code writes each record to standard out to show the keys in each record. For
the other keys, see data/consumer.clj<sup>[6](#consumerdata)</sup>

The [KafkaConsumer javadocs](https://kafka.apache.org/20/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)
give more information about how the consumer works.

```
(ns consumer-example
  (:require
    [jackdaw.client :as jc]
    [jackdaw.client.log :as jl]))

(def consumer-config
  {"bootstrap.servers" "localhost:9092"
   "group.id"  "com.foo.my-consumer"})

(def topic-foo
  {:topic-name "foo"})

(with-open [my-consumer (-> (jc/consumer consumer-config)
                            (jc/subscribe [topic-foo]))]
  (doseq [{:keys [key value partition timestamp offset]} (jl/log my-consumer 500)]
    (println "key: " key)
    (println "value: " value)
    (println "partition: " partition)
    (println "timestamp: " timestamp)
    (println "offset: " offset)))
```

With `subscribed-consumer`, all subscribed topics must use the same pair of key serde instance and value serde instance. The consumer uses the serdes of the first topic in the `topic-configs` vector. If that topic gives no serdes, the consumer uses the serdes from the `consumer-config`. Thus all topics must be able to use the same serdes.

## References

 <a name="producerapi">1</a>: https://kafka.apache.org/documentation/#producerapi <br />
 <a name="consumerapi">2</a>: https://kafka.apache.org/documentation/#consumerapi <br />
 <a name="producerconfig">3</a>: https://kafka.apache.org/documentation/#producerconfigs <br />
 <a name="serdesdirectory">4</a>: https://github.com/FundingCircle/jackdaw/blob/master/src/jackdaw/serdes <br />
 <a name="consumerconfig">5</a>: https://kafka.apache.org/documentation/#consumerconfigs <br />
 <a name="consumerdata">6</a>: https://github.com/FundingCircle/jackdaw/blob/master/src/jackdaw/data/consumer.clj <br />
 <a name="clientapiexample">7</a>: https://github.com/FundingCircle/jackdaw/blob/master/examples/rolldice <br />
