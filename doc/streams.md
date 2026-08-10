# Jackdaw Streams API

## Rationale

The Jackdaw Streams API is a thin wrapper around the Kafka Streams
DSL. It lets you define streaming applications with idiomatic
Clojure functions in place of the Java interop.

Kafka Streams can be a good choice if all of these conditions are true:

- You apply complex transformations or aggregations to one or more data streams.
- The data streams are available as Kafka topics.
- You must make the output highly available.

For a simple transformation, consider
[SMT Transforms](https://docs.confluent.io/current/connect/transforms/index.html)
with Kafka Connect.


## Usage

If you've used the Java API, you'll be aware that the core operators are defined
as methods on the KStream and KTable classes. In Jackdaw, we expose these
methods as functions in the `jackdaw.streams` namespace with names that are
hyphenated versions of the corresponding Java method.

The [API
docs](https://cljdoc.org/d/fundingcircle/jackdaw/CURRENT/api/jackdaw.streams)
should be consulted for full details but the essential elements of a typical
streams app are described below


### Topic Definition

```clojure
(def topic-metadata

  {:input
   {:topic-name "input"
    :partition-count 1
    :replication-factor 1
    :key-serde (jackdaw.serdes.edn/serde)
    :value-serde (jackdaw.serdes.edn/serde)}

   :output
   {:topic-name "output"
    :partition-count 1
    :replication-factor 1
    :key-serde (jackdaw.serdes.edn/serde)
    :value-serde (jackdaw.serdes.edn/serde)}})
```


### App Definition

```clojure
(ns my.example.word-count
  (:require
    [clojure.string :as str]
    [jackdaw.streams :as j]))

(defn split-lines
  [input-string]
  (str/split (str/lower-case input-string) #"\W+"))

(defn topology-builder
  [topic-metadata]
  (fn [builder]
    (let [text-input (j/kstream builder (:input topic-metadata))

          counts (-> text-input
                     (j/flat-map-values split-lines)
                     (j/group-by (fn [[_ v]] v))
                     (j/count))]

      (-> counts
          (j/to-kstream)
          (j/to (:output topic-metadata)))

      builder)))
```


### Start the App

```clojure
(defn -main
  [& args]
  (let [app-config (parse-args args)
        builder (j/streams-builder)
        topology ((topology-builder topic-metadata) builder)
        app (j/kafka-streams topology app-config)]
    (j/start app)
    app))
```
