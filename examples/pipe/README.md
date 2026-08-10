# Pipe

This tutorial has a stream processing application that uses Jackdaw and Kafka Streams.

## Setting up

Before you start, install the Confluent Platform CLI from [https://www.confluent.io/download/](https://www.confluent.io/download/).

To install Clojure: [https://clojure.org/guides/getting_started](https://clojure.org/guides/getting_started).

## Project structure

The project has this structure:
```
$ tree pipe
pipe
├── README.md
├── deps.edn
├── dev
│   └── system.clj
├── src
│   └── pipe.clj
└── test
    └── pipe_test.clj
```

The `deps.edn` file describes the project's dependencies and source paths.

The `system.clj` file has functions to start, stop, and reset the application. The `user` namespace needs these functions for interactive development. Do not call them directly.

The `pipe.clj` file describes the app and topology. Pipe reads from a Kafka topic called "input", logs the key and value, and writes to a Kafka topic called "output":
```
(defn build-topology
  [builder]
  (-> (j/kstream builder (topic-config "input"))
      (j/peek (fn [[k v]]
                (info (str {:key k :value v}))))
      (j/to (topic-config "output")))
  builder)
```

The `pipe_test.clj` file contains a test.

## Running the app

Start a Clojure REPL and load the `pipe` namespace. Then start ZooKeeper and Kafka. Skip this step if the services run already:
```
user> (confluent/start)
INFO zookeeper is up (confluent:288)
INFO kafka is up (confluent:288)
nil
```

Start the application.
```
user> (start)
INFO topic 'input' is created (jackdaw.admin.client:288)
INFO topic 'output' is created (jackdaw.admin.client:288)
INFO pipe is up (pipe:288)
{:app #object[org.apache.kafka.streams.KafkaStreams 0x225dcbb9 "org.apache.kafka.streams.KafkaStreams@225dcbb9"]}
```

The `user/start` function creates two Kafka topics needed by Pipe and starts it.

To get the full list of topics, enter:
```
user> (get-topics)
#{"output" "__confluent.support.metrics" "input"}
```

When the application runs, write a record to the input stream:
```
user> (publish (topic-config "input") nil "this is a pipe")
INFO {:key nil, :value "this is a pipe"} (pipe:288)
nil
```
Pipe logs the key and value to the standard output.

To read from the output stream:
```
user> (get-keyvals (topic-config "output"))
((nil "this is a pipe"))
```

The tutorial is complete.

## Interactive development

For interactive development, reload the file and call `user/reset`. It stops the application and deletes topics and internal state with a regex. It then recreates the topics and starts the application. The `system` namespace has the details.

## Running tests

To run tests, load the `pipe-test` namespace. Call a test runner from your editor or the command line:
```
clj -Atest
```
