(ns jackdaw.serdes.avro.schema-registry
  "Helpers for talking to one of Confluent's Avro schema registries."
  {:license "BSD 3-Clause License <https://github.com/FundingCircle/jackdaw/blob/master/LICENSE>"}
  (:import [io.confluent.kafka.schemaregistry.client
            MockSchemaRegistryClient
            CachedSchemaRegistryClient]
           [io.confluent.kafka.schemaregistry.avro AvroSchemaProvider]
           [io.confluent.kafka.schemaregistry.json JsonSchemaProvider]))

(set! *warn-on-reflection* true)

(defn client
  "Build and return a Kafka Schema Registry client which uses an LRU
  strategy to cache the specified number of schemas."
  [^String url max-capacity]
  {:pre [(string? url)
         (pos-int? max-capacity)]}
  (CachedSchemaRegistryClient. url ^int max-capacity))

(defn mock-client
  "Build and return a mock schema registry client.

  Really suitable only for testing."
  []
  ;; Confluent 8.x no longer registers the JSON/Protobuf providers by default,
  ;; so register Avro + JSON explicitly (jackdaw supports both); without this a
  ;; JSON-schema serde fails with \"Invalid schema type JSON\".
  (MockSchemaRegistryClient. [(AvroSchemaProvider.) (JsonSchemaProvider.)]))
