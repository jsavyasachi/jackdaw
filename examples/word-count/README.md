# Word Count

This example counts words in a stream processing application that uses the
Jackdaw Streams API.


## Setting up

Before you start, download and install the Confluent CLI from
[https://www.confluent.io/download/](https://www.confluent.io/download/).

To install Clojure:
[https://clojure.org/guides/getting_started](https://clojure.org/guides/getting_started).


## Project structure

The project has this structure:
```
tree
.
├── README.md
├── deps.edn
├── dev
│   └── user.clj
├── resources
│   ├── logback.xml -> ../../resources/logback.xml
│   └── metamorphosis.txt
├── src
│   └── word_count.clj
└── test
    └── word_count_test.clj

4 directories, 7 files
```

The `deps.edn` file describes the source paths and dependencies.

The `user.clj` file contains functions to start and stop the
app during interactive development.

The `word_count.clj` file contains the app and topology. The
application reads from a Kafka topic called `input` and splits the
value into words. It then writes to a Kafka topic called `output` for
each word seen:
```
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

The `word_count_test.clj` file contains a test


## Running the app

Install the CLI and start the Confluent Platform:
```
curl -L https://cnfl.io/cli | sh -s -- -b /<path-to-directory>/bin
<path-to-confluent>/bin/confluent local start
```

Then change to the project directory and start a REPL.
```
cd <path-to-jackdaw>/examples/word-count
clj -A:dev
```

The output is similar to this:
```
Clojure 1.10.1
user=>
```

Enter the following at the `user=>` prompt:
```
(reset)
```

The application runs if the output is similar to this:
```
:reloading (word-count user)
:resumed
```

Publish two records and get the result:
```
(publish (:input (topic-metadata)) nil "all streams lead to kafka")
(publish (:input (topic-metadata)) nil "hello kafka streams")
(get-keyvals (:output (topic-metadata)))
```

The output is similar to this:
```
(["all" 1]
 ["streams" 1]
 ["lead" 1]
 ["to" 1]
 ["kafka" 1]
 ["hello" 1]
 ["kafka" 2]
 ["streams" 2])
```

For a detailed walkthrough, see the comment block in the `word_count.clj` file.


## Running tests

To run tests, load the `word-count-test` namespace in your editor and
invoke a test runner, or from the command-line:
```
clj -A:test
```
