# The Test Machine

## Rationale

The test machine helps you write reliable integration tests. A background thread
handles reads and writes. A Clojure ref makes them available. The test author does
not need to manually `send!` and `recv` Kafka messages. The test machine sends events
to the system and collects output with the reader. It supports blackbox integration tests that are:

 * Generative, to avoid manual creation and change of test cases.
 * Reliable, to give results that you can trust.
 * Fast, to give results quickly.

The test machine works with any system that gets input and output mainly from Kafka.

### Construction

The examples below show how to create a test machine that runs a sequence of
commands. The "local-machine" runs the commands against a local Kafka cluster.
All services run on their default ports. The "remote-machine" runs commands over
HTTP through the configured rest-proxy. Use it when you do not have direct access
to services in a shared environment, such as UAT or staging.

```clojure
(ns my.app-test
  (:require
    [my.app :as app]
    [jackdaw.serdes :refer [string-serde edn-serde]]
    [jackdaw.serdes.json :as jsj]
    [jackdaw.test :refer [test-machine]]
    [jackdaw.test.transports :as trns]))

(def local-kafka-config
  {"bootstrap.servers" "localhost:9092"
   "group.id" "my-app"})

(def remote-kafka-config
  {:bootstrap-uri "my-real-rest-proxy-url"
   :group-id "my-app"})

(def topic-config
  {:foo {:topic-name "foo"
         :key-serde (string-serde)
         :value-serde (jsj/serde)
         :partition-count 1
         :replication-factor 1}
   :bar {:topic-name "foo"
         :key-serde (string-serde)
         :value-serde (edn-serde)
         :partition-count 1
         :replication-factor 1}})

(defn local-machine []
  (let [t (trns/transport {:type :kafka
                           :config local-kafka-config
                           :topics topic-config})]
    (test-machine t)))

(defn remote-machine []
  (let [t (trns/transport {:type :confluent-rest-proxy
                           :config remote-kafka-config
                           :topics topic-config})]
   (test-machine t)))
```

### Serialization/Deserialization

The `topic-config` in the example maps topic-ids to serialization and deserialization
configurations. Tests can read and write with the serializers and deserializers that
your applications use. For example, a command such as

```clojure
[:write! :foo {:id 1, :msg "hello"}]
```

causes the test-machine to look up `:foo` in the topic-config. It gets the `:key-serde`
and `:value-serde`, then uses them to write the message. The test machine reads each
topic in the topic-config with its related deserializer.

### Lifecycle

The test-machine implements the `Closeable` protocol. Use it with `with-open` to
make sure that it closes related resources when you finish with a machine.

### Test Commands

Each test-command is a vector. The first item is a keyword for the operation. The
remaining items are arguments for that command. The test machine supports these commands.

```clojure
  :write!   [topic-id msg opts]  Writes a message to the topic (Opts supports :key-fn, :partition, :partition-fn, :key, :timeout)
  :watch    [f opts]             Blocks until `(f @journal)` returns truthy
  :stop     []                   Stops processing commands. All subsequent commands
                                 are ignored
  :sleep    [sleep-ms]           Sleeps for `sleep-ms` milliseconds
  :println  [args]               Prints the supplied args to stdout
  :pprint   [args]               Pretty prints the supplied args to stdout
  :do       [f]                  Execute arbitrary function. The function should take
                                 a single argument, and will be passed the journal
                                 state (content of the journal atom).
  :do!      [f]                  Execute arbitrary function. The function should take
                                 a single argument, and will be passed the journal
                                 atom itself. Allows monitoring of the joural or
                                 the like to be injected.
  :inspect  [f]                  A debugging command, again executes an arbitrary
                                 function of a single argument. This function will
                                 be passed the entire test machine state.
```
For more details, see the functions in the [jackdaw.test.commands](https://cljdoc.org/d/fundingcircle/jackdaw/CURRENT/api/jackdaw.test.commands) namespace.

### Test Results

Use a `test-machine` with `run-test`. The function runs a sequence of test commands
against the test-machine. Its first parameter is a test-machine. Its second parameter is
a list of commands. `run-test` returns a map with two keys:

```clojure
:results   A sequence of execution results. One for each command attempted
:journal   A snapshot of all kafka output read by the test consumer
```

The journal contains all output written to the configured topics, including input
messages. Each journal key represents one topic. Its value is a vector of messages
from that topic in the order that the consumer observes them.

### Fixtures

The library provides fixtures to set up required topics and to start applications
and external systems under test. For more details, see the functions in the
[jackdaw.test.fixtures](https://cljdoc.org/d/fundingcircle/jackdaw/CURRENT/api/jackdaw.test.fixtures) namespace.

### Wrapping up

You can write a function that runs setup and then calls your test function `f` with
a machine. This setup depends on the system under test. Write this macro for your
requirements.

```clojure
(ns my.app-test
  (:require
    [my.app :as app]
    [jackdaw.serdes :refer [string-serde]]
    [jackdaw.test :refer [test-machine]]
    [jackdaw.test.fixtures :refer [with-fixtures topic-fixture service-ready?]])
  (:import
    (org.apache.kafka.streams TopologyTestDriver)))

(def kafka-config
  {"bootstrap.servers" "localhost:9092"
   "group.id" "my-app"})

(def input-topic-config
  {:foo {:topic-name "foo"
         :key-serde (string-serde)
         :value-serde (string-serde)
         :partition-count 1
         :replication-factor 1})

(def output-topic-config
  {:foo {:topic-name "bar"
         :key-serde (string-serde)
         :value-serde (string-serde)
         :partition-count 1
         :replication-factor 1})

(defn with-test-machine
  "Creates a test-machine using the supplied `transport` and then
   passes it to the supplied `f`."
  [f transport]
  (with-fixtures [(topic-fixture kafka-config input-topic-config)
                  (topic-fixture kafka-config output-topic-config)
                  (service-ready? {:http-url "http://localhost:8082"
                                   :timeout 5000})]
    (with-open [machine (test-machine transport)]
      (f machine))))
```

The `topic-fixture` function creates the topics named in `topic-config` before tests run.
Import `TopologyTestDriver` for `test-machine` to work. Add the
`org.apache.kafka/kafka-streams-test-utils` library as a dependency. Use a version from
`2.0.0` to `2.3.0`. The `topic-config` must contain `:topic-name`, `:partition-count`,
`:replication-factor`, and key-value serdes.
