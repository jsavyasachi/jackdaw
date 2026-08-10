# Roll dice

This repository contains an example application that uses Jackdaw's Client API (Consumer and Producer API). The application rolls a die `n` times. The user gives `n`. A Jackdaw Producer writes the numbers to the input topic `rolldice`. A Jackdaw consumer reads the topic, adds the numbers, and prints the result. To stop the consumer loop, press Ctrl+C.

## Installation

Clone this repo.

## Usage

1. Start the Kafka services with `docker-compose up -d`. You can also run the Kafka services (broker and zookeeper) locally. Follow the [Apache Kafka Quickstart](https://kafka.apache.org/quickstart).

2. From the repository root, run `lein run`.
