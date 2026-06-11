(ns jackdaw.test.transports)

(set! *warn-on-reflection* true)

(defonce +transports+ (atom #{}))

(defmulti transport :type)

(defn supported-transports
  "Returns the set of registered test-transport types (the `:type` values that
  `transport` can satisfy)."
  []
  @+transports+)

(defmethod transport :default
  [_cfg]
  (throw (ex-info "unable to find transport to satisfy config" {})))

(defmacro deftransport
  "Defines a `transport` multimethod implementation for `transport-type` and
  registers that type in the supported-transports set."
  [transport-type args & body]
  `(do
     (defmethod transport ~transport-type
       ~args
       ~@body)
     (swap! +transports+ conj ~transport-type)))

(defn with-transport
  "Wires `transport` (its consumer/producer channels) into `machine`, returning
  the merged machine; throws if the transport provides no consumer messages
  channel."
  [machine transport]
  (let [machine' (merge-with concat
                             (dissoc machine :transport)
                             transport)]
    (when-not (get-in machine' [:consumer :messages])
      (throw (ex-info "no messages channel provided by selected transport"
                      {})))
    machine'))
