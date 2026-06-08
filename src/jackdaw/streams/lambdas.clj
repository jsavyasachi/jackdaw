(ns jackdaw.streams.lambdas
  "Wrappers for the Java 'lambda' functions."
  {:license "BSD 3-Clause License <https://github.com/FundingCircle/jackdaw/blob/master/LICENSE>"}
  (:import java.util.function.Function
           org.apache.kafka.streams.KeyValue
           [org.apache.kafka.streams.kstream
            Aggregator ForeachAction Initializer KeyValueMapper
            Merger Predicate Reducer Transformer TransformerSupplier
            ValueJoiner ValueMapper ValueTransformer ValueTransformerSupplier]
           [org.apache.kafka.streams.processor
            StreamPartitioner]
           [org.apache.kafka.streams.processor.api
            Processor ProcessorSupplier ProcessorContext Record
            FixedKeyProcessor FixedKeyProcessorSupplier
            FixedKeyProcessorContext FixedKeyRecord]))

(set! *warn-on-reflection* true)

(defn key-value
  "A key-value pair defined for a single Kafka Streams record."
  [[key value]]
  (KeyValue. key value))

(deftype FnAggregator [aggregator-fn]
  Aggregator
  (apply [_this agg-key value aggregate]
    (aggregator-fn aggregate [agg-key value])))

(defn aggregator
  "Packages up a Clojure fn in a kstream aggregator."
  ^Aggregator [aggregator-fn]
  (FnAggregator. aggregator-fn))

(deftype FnForeachAction [foreach-action-fn]
  ForeachAction
  (apply [_this key value]
    (foreach-action-fn [key value])
    nil))

(defn foreach-action
  "Packages up a Clojure fn in a kstream ForeachAction."
  [foreach-action-fn]
  (FnForeachAction. foreach-action-fn))

(deftype FnInitializer [initializer-fn]
  Initializer
  (apply [_this]
    (initializer-fn)))

(defn initializer
  "Packages up a Clojure fn in a kstream Initializer."
  ^Initializer [initializer-fn]
  (FnInitializer. initializer-fn))

(deftype FnKeyValueMapper [key-value-mapper-fn]
  KeyValueMapper
  (apply [_this key value]
    (key-value (key-value-mapper-fn [key value]))))

(defn key-value-mapper
  "Packages up a Clojure fn in a kstream key value mapper."
  [key-value-mapper-fn]
  (FnKeyValueMapper. key-value-mapper-fn))

(deftype FnSelectKeyValueMapper [select-key-value-mapper-fn]
  KeyValueMapper
  (apply [_this key value]
    (select-key-value-mapper-fn [key value])))

(defn select-key-value-mapper
  "Packages up a Clojure fn in a kstream key value mapper for use with
  `select-key`."
  [select-key-value-mapper-fn]
  (FnSelectKeyValueMapper. select-key-value-mapper-fn))

(deftype FnKeyValueFlatMapper [key-value-flatmapper-fn]
  KeyValueMapper
  (apply [_this key value]
    (mapv key-value (key-value-flatmapper-fn [key value]))))

(defn key-value-flatmapper
  "Packages up a Clojure fn in a kstream key value mapper for use with .flatMap.

  `key-value-flatmapper-fn` should be a function that takes a `[key value]` as a
  single parameter, and returns a list of `[key value]`."
  [key-value-flatmapper-fn]
  (FnKeyValueFlatMapper. key-value-flatmapper-fn))

(deftype FnMerger [merger-fn]
  Merger
  (apply [_this agg-key aggregate1 aggregate2]
    (merger-fn agg-key aggregate1 aggregate2)))

(defn merger
  "Packages up a Clojure fn in a kstream merger (merges together two SessionWindows aggregate values)."
  ^Merger [merger-fn]
  (FnMerger. merger-fn))

(deftype FnPredicate [predicate-fn]
  Predicate
  (test [_this key value]
    (boolean (predicate-fn [key value]))))

(defn predicate
  "Packages up a Clojure fn in a kstream predicate."
  [predicate-fn]
  (FnPredicate. predicate-fn))

(deftype FnReducer [reducer-fn]
  Reducer
  (apply [_this value1 value2]
    (reducer-fn value1 value2)))

(defn reducer
  "Packages up a Clojure fn in a kstream reducer."
  ^Reducer [reducer-fn]
  (FnReducer. reducer-fn))

(deftype FnValueJoiner [value-joiner-fn]
  ValueJoiner
  (apply [_this value1 value2]
    (value-joiner-fn value1 value2)))

(deftype FnForeignKeyExtractor [foreign-key-extractor-fn]
  Function
  (apply [_this value]
    (foreign-key-extractor-fn value)))

(defn foreign-key-extractor
  "Packages up a Clojure fn into a Java Function - hopefully, redundant as of Clojure 1.12."
  [foreign-key-extractor-fn]
  (FnForeignKeyExtractor. foreign-key-extractor-fn))

(defn value-joiner
  "Packages up a Clojure fn in a kstream value joiner."
  [value-joiner-fn]
  (FnValueJoiner. value-joiner-fn))

(deftype FnValueMapper [value-mapper-fn]
  ValueMapper
  (apply [_this value]
    (value-mapper-fn value)))

(defn value-mapper
  "Packages up a Clojure fn in a kstream value mapper."
  [value-mapper-fn]
  (FnValueMapper. value-mapper-fn))

(deftype FnStreamPartitioner [stream-partitioner-fn]
  StreamPartitioner
  ;; Kafka 4.0 removed the deprecated `Integer partition(...)` method; the
  ;; interface now exposes `Optional<Set<Integer>> partitions(...)`. Preserve
  ;; jackdaw's single-partition fn contract by wrapping its result in a
  ;; singleton set (empty Optional => fall back to the default partitioner).
  (partitions [_this topic-name key val partition-count]
    (if-let [p (stream-partitioner-fn topic-name key val partition-count)]
      (java.util.Optional/of #{(int p)})
      (java.util.Optional/empty))))

(defn stream-partitioner
  "Packages up a Clojure fn in a kstream partitioner."
  [stream-partitioner-fn]
  (when stream-partitioner-fn
    (FnStreamPartitioner. stream-partitioner-fn)))

(deftype FnProcessor [context processor-fn]
  Processor
  (close [_])
  (init [_ processor-context]
    (reset! context processor-context))
  (process [_ record]
    (processor-fn @context (.key record) (.value record))))

(defn processor
  "Packages up a Clojure fn as a kstream processor."
  [processor-fn]
  (FnProcessor. (atom nil) processor-fn))

(deftype FnProcessorSupplier [processor-supplier-fn]
  ProcessorSupplier
  (get [_this]
    (processor processor-supplier-fn)))

(defn processor-supplier
  "Packages up a Clojure fn in a kstream processor supplier."
  ^ProcessorSupplier [processor-fn]
  (FnProcessorSupplier. processor-fn))

(deftype FnTransformerSupplier [transformer-supplier-fn]
  TransformerSupplier
  (get [_this]
    (transformer-supplier-fn)))

(defn transformer-supplier
  "Packages up a Clojure fn in a kstream transformer supplier."
  [transformer-supplier-fn]
  (FnTransformerSupplier. transformer-supplier-fn))

(deftype FnValueTransformerSupplier [value-transformer-supplier-fn]
  ValueTransformerSupplier
  (get [_this]
    (value-transformer-supplier-fn)))

(defn value-transformer-supplier
  "Packages up a Clojure fn in a kstream value transformer supplier."
  [value-transformer-supplier-fn]
  (FnValueTransformerSupplier. value-transformer-supplier-fn))

(defprotocol IContextualTransformer
  "Lets the Processor-API adapters inject the live (api) ProcessorContext into
  jackdaw's context-aware transformer sugar. Kafka 4.0 removed the legacy
  Transformer.init(ProcessorContext); the new context is supplied here instead."
  (set-transformer-context! [this ctx]))

(deftype FnTransformer [context xfm-fn]
  Transformer
  ;; Kafka 4.0 removed the legacy ProcessorContext passed to Transformer.init;
  ;; the adapter injects the new api ProcessorContext via set-transformer-context!.
  (init [_this _transformer-context])
  (close [_this])
  (transform [_this k v]
    (xfm-fn @context k v))
  IContextualTransformer
  (set-transformer-context! [_this ctx]
    (reset! context ctx)))

(defn transformer-with-ctx
  "Helper to create a Transformer for use inside the jackdaw transform wrapper.
  Passed function should take three args - the context, key and value for the stream.
  The processor context allows access to stream internals such as state stores.
  Result is returned from the transform. E.g.
  ```
  (-> builder
      (k/stream topic)
      (k/transform
        (kl/transformer-with-ctx
          (fn [ctx k v]
            ...))))
  ```"
  [xfm-fn]
 (fn [] (FnTransformer. (atom nil) xfm-fn)))

(deftype FnValueTransformer [context xfm-fn]
  ValueTransformer
  (init [_this _transformer-context])
  (close [_this])
  (transform [_this v]
    (xfm-fn @context v))
  IContextualTransformer
  (set-transformer-context! [_this ctx]
    (reset! context ctx)))

(defn value-transformer-with-ctx
  "Helper to create a ValueTransformer for use inside the jackdaw transform-values wrapper.
  Passed function should take two args - the context and value for the stream.
  The processor context allows access to stream internals such as state stores.
  Result is returned from the transform-values. E.g.
  ```
  (-> builder
      (k/stream topic)
      (k/transform-values
        (kl/value-transformer-with-ctx
          (fn [ctx v]
            ...))))
  ```"
  [xfm-fn]
  (fn [] (FnValueTransformer. (atom nil) xfm-fn)))

;; Kafka 4.0 (KIP-820) removed KStream.transform/flatTransform/transformValues/
;; flatTransformValues. The Transformer/ValueTransformer interfaces still exist,
;; so adapt them onto the Processor API (process/processValues). The transformer's
;; returned value(s) are forwarded; the legacy ProcessorContext is gone, so the
;; transformer's init receives nil - transformers that read the context must
;; migrate to process! / a FixedKeyProcessor.

(defn transformer-supplier->processor-supplier
  "Adapt a TransformerSupplier onto a Processor API ProcessorSupplier.
  `flat?` true => transform returns a seq of KeyValue; false => a single
  KeyValue (nil drops the record)."
  ^ProcessorSupplier [^TransformerSupplier supplier flat?]
  (reify ProcessorSupplier
    (get [_]
      (let [transformer (volatile! nil)
            ctx (volatile! nil)]
        (reify Processor
          (init [_ context]
            (vreset! ctx context)
            (let [t (.get supplier)]
              (vreset! transformer t)
              (when (satisfies? IContextualTransformer t)
                (set-transformer-context! t context))
              (.init ^Transformer t nil)))
          (process [_ record]
            (let [^Record record record
                  result (.transform ^Transformer @transformer
                                     (.key record) (.value record))
                  forward1 (fn [^KeyValue kv]
                             (.forward ^ProcessorContext @ctx
                                       (.withValue (.withKey record (.key kv))
                                                   (.value kv))))]
              (when result
                (if flat?
                  (doseq [kv result] (forward1 kv))
                  (forward1 result)))))
          (close [_]
            (when-let [t @transformer] (.close ^Transformer t))))))))

(defn value-transformer-supplier->fk-processor-supplier
  "Adapt a ValueTransformerSupplier onto a FixedKeyProcessorSupplier
  (processValues, key preserved). `flat?` true => transform returns a seq of
  values; false => a single value (nil drops the record)."
  ^FixedKeyProcessorSupplier [^ValueTransformerSupplier supplier flat?]
  (reify FixedKeyProcessorSupplier
    (get [_]
      (let [transformer (volatile! nil)
            ctx (volatile! nil)]
        (reify FixedKeyProcessor
          (init [_ context]
            (vreset! ctx context)
            (let [t (.get supplier)]
              (vreset! transformer t)
              (when (satisfies? IContextualTransformer t)
                (set-transformer-context! t context))
              (.init ^ValueTransformer t nil)))
          (process [_ record]
            (let [^FixedKeyRecord record record
                  result (.transform ^ValueTransformer @transformer (.value record))]
              (when result
                (if flat?
                  (doseq [v result]
                    (.forward ^FixedKeyProcessorContext @ctx (.withValue record v)))
                  (.forward ^FixedKeyProcessorContext @ctx (.withValue record result))))))
          (close [_]
            (when-let [t @transformer] (.close ^ValueTransformer t))))))))
