(ns jackdaw.streams.describe
  (:require [clj-uuid :as uuid]
            [clojure.string :as str])
  (:import
   (org.apache.kafka.streams TopologyDescription
                             TopologyDescription$Node
                             TopologyDescription$Source
                             TopologyDescription$Sink
                             TopologyDescription$Processor
                             TopologyDescription$GlobalStore
                             TopologyDescription$Subtopology)))

(set! *warn-on-reflection* true)

(defn ->edge
  [from to]
  {:from from :to to})

(defn base-node
  "Returns the base graph fragment `{:nodes :edges}` for topology node `n` typed
  `t`, with edges connecting it to its predecessor and successor nodes."
  [t n]
  {:nodes [{:type t
            :name (.name ^TopologyDescription$Node n)}]
   :edges (concat
           (map #(->edge (.name ^TopologyDescription$Node %)
                         (.name ^TopologyDescription$Node n))
                (.predecessors ^TopologyDescription$Node n))
           (map #(->edge (.name  ^TopologyDescription$Node n)
                         (.name ^TopologyDescription$Node %))
                (.successors ^TopologyDescription$Node n)))})

(defn describe-node-dispatch
  "Dispatch fn for `describe-node`: the lower-cased simple class name of node `n`
  as a keyword (e.g. :source, :sink, :processor, :globalstore, :subtopology)."
  [n]
  (keyword (str/lower-case (.getSimpleName ^Class (.getClass ^Object n)))))

(defmulti describe-node describe-node-dispatch)

(defmethod describe-node :node [n]
  (base-node :node n))

(defmethod describe-node :source [n]
  ;; Kafka 4.0 removed TopologyDescription.Source.topics() (a String); use
  ;; topicSet() which returns the topic names directly as a Set<String>.
  (let [topics (seq (.topicSet ^TopologyDescription$Source n))]
    (-> (base-node :source n)
        (update :nodes concat (map (fn [t]
                                     {:type :topic
                                      :name t}) topics))
        (update :edges concat (map #(->edge % (.name ^TopologyDescription$Source n))
                                   topics)))))

(defmethod describe-node :sink [n]
  (-> (base-node :sink n)
      (update :nodes conj {:type :topic
                           :name (.topic ^TopologyDescription$Sink n)})
      (update :edges conj (->edge (.name ^TopologyDescription$Sink n)
                                  (.topic ^TopologyDescription$Sink n)))))

(defmethod describe-node :processor [n]
  (let [stores (.stores ^TopologyDescription$Processor n)]
    (-> (base-node :processor n)
        (update :nodes concat (map (fn [t]
                                     {:type :store
                                      :name (str "localstore-" t)}) stores))
        (update :edges concat (map #(->edge (.name ^TopologyDescription$Processor n)
                                            (str "localstore-" %))
                                   stores)))))

(defmethod describe-node :globalstore [n]
  (let [source (describe-node (.source ^TopologyDescription$GlobalStore n))
        processor (describe-node (.processor ^TopologyDescription$GlobalStore n))]
    {:type :globalstore
     :name (str "globalstore-" (.id ^TopologyDescription$GlobalStore n))
     :nodes (set (mapcat :nodes [source processor]))
     :edges (set (mapcat :edges [source processor]))}))

(defmethod describe-node :subtopology [n]
  (let [nodes (map describe-node (.nodes ^TopologyDescription$Subtopology n))]
    {:type :stream
     :name (str "stream-" (.id ^TopologyDescription$Subtopology n))
     :nodes (set (mapcat :nodes nodes))
     :edges (set (mapcat :edges nodes))}))

(defn topic?
  "Returns true if graph node `s` is a topic node (`:type :topic`)."
  [s]
  (= :topic (:type s)))

(defn gen-id
  "Returns a deterministic v5 UUID for graph node/graph `n` within
  `applicaton-id` (topic nodes use a global namespace instead), so the same node
  in the same application always gets the same id across describe calls and can
  be merged across applications."
  [applicaton-id n]
  ;; Take a base UUID from the application id, or a global one for topics
  (let [ns-id (uuid/v5 uuid/+null+ (if (topic? n)
                                     "topics" ; topics are global
                                     applicaton-id))]
    ;; generate a deterministic v5 UUID for the node name for this applicaton-id
    ;; means the same node in the same app gets the same id, but a node with the
    ;; same name in a different app gets a non matching UUID.
    ;; This is required so we can merge the graphs across applicaton-ids.
    (uuid/v5 ns-id (:name n))))

(defn assign-id
  "Assigns a deterministic `:id` (via `gen-id`) to graph node `n`."
  [applicaton-id n]
  (assoc n :id (gen-id applicaton-id n)))

(defn assign-ids
  "Assigns deterministic `:id`s to graph `g` and each of its nodes, then resolves
  every edge's `:from-id`/`:to-id` from the node names."
  [applicaton-id g]
  (let [g* (-> (update g :nodes (fn [v]
                                  (map (partial assign-id applicaton-id) v)))
               (assoc :id (gen-id applicaton-id g)))
        lookup (into {} (map (fn [v]
                               [(:name v) v]) (:nodes g*)))]
    (update g* :edges (fn [v]
                        (map (fn [e]
                               (assoc e
                                      :from-id (:id (lookup (:from e)))
                                      :to-id (:id (lookup (:to e))))) v)))))

(defn is-merge?
  "Returns true if node name `n` is a Kafka Streams merge node (\"KSTREAM-MERGE...\")."
  [n]
  (str/starts-with? n "KSTREAM-MERGE"))

(defn good-edge
  "Returns true if edge `e` is not a self-loop (`:from-id` differs from `:to-id`)."
  [e]
  (not= (:from-id e) (:to-id e)))

(defn collapse-merge-chains
  "Collapses a chain of pairwise Kafka merge nodes in graph `g` into a single
  N-way merge, rewriting edges onto the head merge node and pruning the redundant
  intermediate nodes."
  [g]
  ;; all kafka merges are pairwise, so if you merge lots of topics the graph ends up with
  ;; a long chain of merges all in a row which is messy. This collapses chains of pair-wise
  ;; merges into a single N-way merge
  (let [merge-to-merge-edges (filter (fn [{:keys [from to]}]
                                       (and (is-merge? from)
                                            (is-merge? to))) (:edges g))
        start-id (:from-id (first
                            (filter (fn [{:keys [from-id]}]
                                      (not-any? (fn [{:keys [to-id]}]
                                                  (= from-id to-id)) merge-to-merge-edges))
                                    merge-to-merge-edges)))
        ;; Collapse all the merge nodes in the chain into the merge node at the head of the starting edge.
        ;; All references from edges to the 'collapsed' nodes need to be changed to the head node.
        ;; Remove any pruned nodes and edges as a result.

        ;; generate remappings
        remappings (into {} (map (fn [v]
                                   [v start-id])
                                 (remove #(= % start-id)
                                         (mapcat (fn [{:keys [from-id to-id]}]
                                                   [from-id to-id]) merge-to-merge-edges))))
        pruned-ids (set (keys remappings))]

    (-> g
        (update :edges (fn [edges]
                         (filter good-edge
                                 (map (fn [{:keys [from-id to-id] :as e}]
                                        (assoc e
                                               :from-id (remappings from-id from-id)
                                               :to-id (remappings to-id to-id)))
                                      edges))))
        (update :nodes (fn [nodes]
                         (filter (fn [n]
                                   (not (contains? pruned-ids (:id n))))
                                 nodes))))))

(defn parse-description
  "Parses a Kafka `TopologyDescription` `d` into a sequence of id-assigned,
  merge-collapsed stream graphs (one per subtopology and per global store) scoped
  to `applicaton-id`."
  [applicaton-id d]
  (let [parser (comp collapse-merge-chains
                     (partial assign-ids applicaton-id)
                     describe-node)]
    (concat (map parser (.subtopologies ^TopologyDescription d))
            (map parser (.globalStores ^TopologyDescription d)))))

;; Turn off reflection warning for this last function as it takes
;; two hetrogeneous types as valid input
(set! *warn-on-reflection* false)

(defn describe-topology
  "Returns a list of the stream graphs in a topology.
  The passed in topology object must have a `describe` method, meaning
  it is one of:

  Kafka >= 1.1 : https://kafka.apache.org/21/javadoc/org/apache/kafka/streams/Topology.html
  Kafka <  1.1 : https://kafka.apache.org/10/javadoc/org/apache/kafka/streams/processor/TopologyBuilder.html#internalTopologyBuilder

  Each stream graph takes the form:

  {:id    <a unique UUID for the stream, deterministic from the encosing topology and its stream name>
   :type  :stream
   :name  <the name that kafka gives this stream>
   :nodes <a list of all the nodes in the graph>
   :edges <a list of all the edges in the graph>}

  Nodes and edges are represented as:

  {:id   <a deterministic UUID for the node>
   :name <the name as assigned by kafka>
   :type <the type - processor, store, topic &c.>}

  {:from    <the :name of the node the edge comes from>
   :from-id <the :id of the node the edge comes from>
   :to      <the :name of the node the edge goes to>
   :to-id   <the :id of the node the edge goes to>}

  All identifiers are v5 UUIDs, and are globally unique where objects
  are distinct and globally equal where objects are the same."
  [topology streams-config]
  (parse-description (get streams-config "application.id") (.describe topology)))
