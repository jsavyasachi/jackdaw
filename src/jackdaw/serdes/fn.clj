(ns jackdaw.serdes.fn
  "FIXME"
  {:license "BSD 3-Clause License <https://github.com/FundingCircle/jackdaw/blob/master/LICENSE>"}
  (:require [clojure.spec.alpha :as s]
            [jackdaw.serdes.fn-impl :as fn-impl])
  (:import [org.apache.kafka.common.serialization Deserializer Serializer]))

(set! *warn-on-reflection* true)

(s/def ::serialize fn?)
(s/def ::close fn?)
(s/def ::configure fn?)

(s/fdef new-serializer :args (s/cat :args (s/keys :req-un [::serialize]
                                                  :opt-un [::close
                                                           ::configure])))

(defn new-serializer
  "Returns a Kafka Serializer backed by the functions in `args`: required
  `:serialize` (fn of [topic data]) and optional `:close` / `:configure`."
  ^Serializer [args]
  (fn-impl/map->FnSerializer args))

(s/def ::deserialize fn?)
(s/fdef new-deserializer :args (s/cat :args (s/keys :req-un [::deserialize]
                                                    :opt-un [::close
                                                             ::configure])))

(defn new-deserializer
  "Returns a Kafka Deserializer backed by the functions in `args`: required
  `:deserialize` (fn of [topic data]) and optional `:close` / `:configure`."
  ^Deserializer [args]
  (fn-impl/map->FnDeserializer args))

