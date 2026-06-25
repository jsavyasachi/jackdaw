# jackdaw

[![Clojars Project](https://img.shields.io/clojars/v/net.clojars.savya/jackdaw.svg)](https://clojars.org/net.clojars.savya/jackdaw)
[![cljdoc](https://cljdoc.org/badge/net.clojars.savya/jackdaw)](https://cljdoc.org/d/net.clojars.savya/jackdaw/CURRENT)
[![test](https://github.com/jsavyasachi/jackdaw/actions/workflows/build.yaml/badge.svg)](https://github.com/jsavyasachi/jackdaw/actions/workflows/build.yaml)
[![Renovate](https://img.shields.io/badge/Renovate-enabled-1A1F6C?style=flat&logo=renovate&logoColor=fff)](https://github.com/jsavyasachi/jackdaw/issues?q=is%3Aissue+Dependency+Dashboard)

Jackdaw is a Clojure library for the Apache Kafka distributed streaming platform. With Jackdaw, you can create and list topics using the AdminClient API, produce and consume records using the Producer and Consumer APIs, and create stream processing applications using the Streams API. Jackdaw also contains functions to serialize and deserialize records as JSON, EDN, and Avro, as well as functions for writing unit and integration tests.

> **Maintenance fork.** This is a maintained continuation of
> [`fundingcircle/jackdaw`](https://github.com/fundingcircle/jackdaw) (unmaintained
> since 2024), modernized for **Apache Kafka 4.x**. It is published under a new
> coordinate, `net.clojars.savya/jackdaw`. See the [CHANGELOG](CHANGELOG.md) for the
> 4.x migration notes.

## Stack

<a href="https://clojure.org"><img src="https://img.shields.io/badge/Clojure-5881D8?style=flat&logo=clojure&logoColor=fff" alt="Clojure" /></a>
<a href="https://kafka.apache.org"><img src="https://img.shields.io/badge/Apache%20Kafka-231F20?style=flat&logo=apachekafka&logoColor=fff" alt="Apache Kafka" /></a>

## Installation

Leiningen / Boot:

```clojure
[net.clojars.savya/jackdaw "1.3.5"]
```

deps.edn:

```clojure
net.clojars.savya/jackdaw {:mvn/version "1.3.5"}
```

Jackdaw resolves Confluent artifacts from the Confluent Maven repository; add
`https://packages.confluent.io/maven/` to your `:repositories` (Leiningen) or
`:mvn/repos` (deps.edn).

## Supported versions

Jackdaw 1.3.5 requires **Clojure >= 1.10**, **JDK 17+**, and **Apache Kafka 4.x** /
**Confluent Platform 8.x** brokers. (The Clojure floor is set by the `datafy` protocol,
introduced in 1.10.)

## Documentation

You can find all the documentation on [cljdoc](https://cljdoc.org/d/net.clojars.savya/jackdaw).

## Examples

- [Pipe](https://github.com/jsavyasachi/jackdaw/tree/main/examples/pipe)
- [Word Count](https://github.com/jsavyasachi/jackdaw/tree/main/examples/word-count)
- [Simple Ledger](https://github.com/jsavyasachi/jackdaw/tree/main/examples/simple-ledger)
- [Roll Dice](https://github.com/jsavyasachi/jackdaw/tree/main/examples/rolldice)

## Contributing

We welcome any thoughts or patches - [open an issue](https://github.com/jsavyasachi/jackdaw/issues) on this fork.

## Related projects

If you want to get more insight about your topologies, you can use the
[Topology Grapher](https://github.com/FundingCircle/topology-grapher) library to generate graphs.
See [an example using jackdaw](https://github.com/FundingCircle/topology-grapher/blob/master/sample_project/src/jackdaw_topology.clj) to check how to integrate it with your topology.

## Releasing

This fork self-publishes to Clojars under `net.clojars.savya/jackdaw`.

1. Bump the version in `project.clj` and add a dated entry to `CHANGELOG.md`.
2. Make sure the test suite is green and `lein check` reports no reflection warnings.
3. Deploy: `CLOJARS_USERNAME=<user> CLOJARS_PASSWORD=<clojars-deploy-token> lein deploy clojars`.
4. Tag the release: `git tag <version> && git push --tags`.

## License

Copyright © 2017 Funding Circle

Maintenance fork (2026) by [Savyasachi](https://github.com/jsavyasachi); original:
https://github.com/fundingcircle/jackdaw

Distributed under the BSD 3-Clause License.
 
