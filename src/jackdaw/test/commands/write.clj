(ns jackdaw.test.commands.write
  (:require
   [manifold.stream :as s]
   [jackdaw.client.partitioning :as partitioning]))

(set! *warn-on-reflection* true)

(defn default-partition-fn
  "Returns the partition (0..partition-count) for key `k` on `topic-map`, using
  Kafka's default partitioning strategy."
  [topic-map _topic-name k _v partition-count]
  (int (partitioning/default-partition topic-map k nil partition-count)))

(defn create-message
  "Builds a producer-record map from `message` for `topic-map`: derives the key
  (via `:key-fn`, default `:id`) and the partition (via `:partition-fn`, default
  Kafka's), with explicit `:key`/`:partition`/`:key-fn`/`:partition-fn` in `opts`
  overriding the topic map. Throws if the resulting partition is out of range."
  [topic-map message opts]
  ;; By default the message will use the `:id` field as the key on kafka
  ;; and run the default partitioning function for the partition (which
  ;; works the same as the kafka one). This behaviour can be changed as follows:
  ;;  - If the topic map contains a `:key-fn`, use that function to extract
  ;;    the key from the message
  ;;  - If the topic map contains a `:partition-fn`, use that function to
  ;;    determine the partition to write to (should be an fn of airity 2
  ;;    as per `default-partition-fn`
  ;;  - The `:key-fn` and `:partition-fn` can also be passed separately in
  ;;    the options map `opts`.
  ;;  - Further, the `opts` map can contain an explciit `:key` and/or
  ;;    `:partition`, which if set will provide the values to use
  ;; If both specified, `opts` values will override values in the topic map.
  (let [key-fn (or (:key-fn opts)
                   (:key-fn topic-map)
                   :id)
        partition-fn (or (:partition-fn opts)
                         (:partition-fn topic-map)
                         (partial default-partition-fn topic-map))
        k (if-let [explicit-key (:key opts)]
            explicit-key
            (key-fn message))
        partn (if-let [explicit-partition (:partition opts)]
                explicit-partition
                (partition-fn (:topic-name topic-map) k message (:partition-count topic-map)))
        timestamp (:timestamp opts (System/currentTimeMillis))
        headers (:headers opts)]
    (if (or (< partn 0)
            (> partn (dec (:partition-count topic-map))))
      (throw (ex-info "Invalid partition number for topic"
                      {:partition partn
                       :topic topic-map}))
      {:topic topic-map
       :key k
       :value message
       :partition partn
       :timestamp timestamp
       :headers headers})))

(defn do-write
  "Produces `message` to `topic-name` through the test machine's producer and
  waits up to `opts`'s `:timeout` ms (default 1000) for the ack. Returns an
  `:error` map for an unknown topic or a timeout."
  ([machine topic-name message]
   (do-write machine topic-name message {}))
  ([machine topic-name message opts]
   (if-let [topic-map (get (:topics machine) topic-name)]
     (let [to-send (create-message topic-map message opts)
           messages (:messages (:producer machine))
           ack (promise)]
       (s/put! messages (assoc to-send :ack ack))
       (deref ack (:timeout opts 1000) {:error :timeout}))
     {:error :unknown-topic
      :topic topic-name
      :known-topics (keys (:topic-config machine))})))


(defn handle-write-cmd
  "Handler for the `:write!` command; applies `do-write` to `machine` and the
  `[topic-name message opts?]` in `params`."
  [machine params]
  (apply do-write machine params))

(def command-map
  {:write! handle-write-cmd})
