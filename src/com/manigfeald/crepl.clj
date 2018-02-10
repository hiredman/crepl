(ns com.manigfeald.crepl
  (:require  [clojure.tools.nrepl.transport :as t]
             [clojure.tools.nrepl.misc :refer [response-for]]
             [clojure.tools.nrepl :as repl]
             [clojure.tools.nrepl.server :refer [start-server
                                                 default-handler
                                                 default-middlewares]]
             [clojure.tools.nrepl.middleware.session :refer [session] :as x]))

(def rtt 1000)

(def k 3)

(defn jitter []
  (long (* rtt (min 0.85 (max 0.5 (rand))))))

(defn step [state event]
  (case [state event]
    [nil :ping] :alive
    [nil :timeout] :suspect
    [:alive :ping] :alive
    [:alive :timeout] :suspect
    [:suspect :timeout] :dead
    [:suspect :ping] :alive
    [:dead :timeout] :dead
    [:dead :ping] :alive))

(def l (agent nil))

(defn log [& args]
  (send-off l (fn [_]
                (apply prn args)
                nil)))

(defn neswim [me peers publish receive]
  (assert (every? #(not (empty? %)) peers))
  (let [n (java.util.concurrent.atomic.AtomicLong.)
        responses (java.util.concurrent.ConcurrentHashMap.)
        exec (java.util.concurrent.Executors/newScheduledThreadPool 2)
        peers (atom (into {} (for [peer peers] [peer :alive])))
        publish (fn
                  ([msg]
                     (publish msg))
                  ([msg response-handler]
                     (.put responses
                           (:seq-n msg)
                           (fn [msg]
                             (.remove responses (:seq-n msg))
                             (response-handler msg)))
                     (publish msg)))
        to-gossip (java.util.concurrent.LinkedBlockingQueue.)
        to-check (java.util.concurrent.LinkedBlockingQueue.)]
    (letfn [(reporting [fun]
              (fn [] (try (fun) (catch Throwable e (prn e)))))
            (execute [fun]
              (.execute exec (reporting fun)))
            (schedule [fun delay]
              (.schedule exec
                         ^Runnable (reporting fun)
                         (long delay)
                         java.util.concurrent.TimeUnit/MILLISECONDS))
            (gossip-info []
              (when (.isEmpty to-gossip)
                (.addAll to-gossip (shuffle (seq @peers))))
              (.take to-gossip))
            (responder []
              (receive
               (fn [msg]
                 (when (:other msg)
                   (swap! peers conj (:other msg)))
                 (swap! peers assoc (:from msg) :alive)
                 (.put to-gossip [(:from msg) :alive])
                 (case (:type msg)
                   :ping (publish {:from me :type :response :seq-n (:seq-n msg) :to (:from msg)
                                   :other (gossip-info)})
                   :indirect-ping (publish {:from me :to (:r msg) :type :ping
                                            :seq-n (.incrementAndGet n)
                                            :other (gossip-info)}
                                           (fn [_]
                                             (publish {:from me
                                                       :to (:from msg)
                                                       :type :response
                                                       :other (gossip-info)
                                                       :seq-n (:seq-n msg)})))
                   :response (let [seq-n (long (:seq-n msg))]
                               (when-let [handler (.get responses seq-n)]
                                 (execute #(handler msg)))))
                 (execute responder))))
            (failure-detector []
              (log me "peers" (frequencies (vals @peers)))
              (swap! peers (fn [m] (into {} (for [[k v] m :when (not= v :dead)] [k v]))))
              (when (.isEmpty to-check)
                (.addAll to-check (for [[k v] @peers :when (= v :alive)] k)))
              (when (seq @peers)
                (let [peer (.take to-check)
                      seq-n (.incrementAndGet n)
                      f (schedule (fn []
                                    (swap! peers update peer step :timeout)
                                    (when (= (get @peers peer) :suspect)
                                      (indirect-ping peer)))
                                  rtt)]
                  (publish {:from me :to peer :type :ping :seq-n seq-n :other (gossip-info)}
                           (fn [_]
                             (future-cancel f)
                             (swap! peers update peer step :ping)))))
              (schedule failure-detector (jitter)))
            (indirect-ping [peer]
              (when (seq @peers)
                (let [ipeers (take 3 (remove #{peer} (shuffle (keys @peers))))
                      seq-n (.incrementAndGet n)
                      f (schedule (fn []
                                    (.remove responses seq-n)
                                    (swap! peers update peer step :timeout))
                                  rtt)]
                  (.put responses seq-n (fn [_]
                                          (.remove responses seq-n)
                                          (future-cancel f)
                                          (swap! peers update peer step :ping)))
                  (doseq [ipeer ipeers]
                    (publish {:from me :to ipeer :type :indirect-ping
                              :seq-n seq-n :r peer :other (gossip-info)})))))]
      (execute responder)
      (execute failure-detector)
      (reify
        java.util.concurrent.Executor
        (execute [_ task]
          (.execute exec task))
        clojure.lang.IDeref
        (deref [_]
          (into #{} (for [[k v] @peers
                          :when (= v :alive)]
                      k)))
        java.io.Closeable
        (close [_]
          (.shutdownNow exec))))))

(declare ^:dynamic *cluster*)
(declare ^:dynamic *here*)

(defn cluster [me create-client seed]
  (with-meta
    (fn [h]
      (let [in (java.util.concurrent.ArrayBlockingQueue. 5)
            exec (promise)
            s (neswim
               me
               seed
               (fn [data]
                 (.execute ^java.util.concurrent.Executor  @exec
                           #(with-open [^java.io.Closeable conn (create-client (:to data))]
                              (-> (repl/client conn 1000)
                                  (repl/message
                                   {:op :swim :data (pr-str data)})))))
               (fn [cb]
                 (.execute ^java.util.concurrent.Executor  @exec #(cb (.take in)))))
            _ (deliver exec s)
            p (atom nil)]
        (fn [{:keys [op transport] :as msg}]
          (if (= "swim" op)
            (try
              (.offer in (read-string (:data msg)))
              (t/send transport
                      (-> msg
                          (dissoc :data :transport)
                          (assoc :status :done)))
              (catch Exception _))
            (binding [*cluster* (fn [] @s)
                      *here* me]
              (h msg))))))
    {:clojure.tools.nrepl.middleware/descriptor {:expects #{#'session}}}))


(declare ^:dynamic *proxy-state*)

(defn proxy-state [h]
  (fn [{:keys [p transport] :as msg}]
    (binding [*proxy-state* (atom {:created-at (System/currentTimeMillis)})]
      (h msg))))

(alter-meta! #'proxy-state
             assoc :clojure.tools.nrepl.middleware/descriptor {:expects #{#'session}})

(defn proxy-cutout [h]
  (fn this-fn [{:keys [p transport session] :as msg}]
    (when session
      (swap! (get @session #'*proxy-state*) assoc :now (System/currentTimeMillis)))
    (if-not session
      (h msg)
      (let [proxy-state (get @session #'*proxy-state*)
            ^java.io.Closeable connection (:connection @proxy-state)]
        (if connection
          (if-let [client (:client @proxy-state)]
            (try
              (doseq [new-msg (repl/message client (assoc (dissoc msg :transport :session)
                                                     :session (:session-id @proxy-state)))]
                (t/send transport new-msg))
              (catch Exception e
                (swap! proxy-state dissoc :connection :client)
                (.close connection)
                (t/send transport
                        (clojure.tools.nrepl.misc/response-for
                         msg
                         {:status :eval-error
                          :ex (-> e class str)
                          :root-ex (-> e class str)}))))
            (let [client (repl/client connection 1000)
                  replies (doall (repl/message client {:op :clone}))
                  session-id (:new-session (last replies))]
              (swap! (get @session #'*proxy-state*) assoc :client client :session-id session-id)
              (this-fn msg)))
          (h msg))))))

(alter-meta! #'proxy-cutout
             assoc :clojure.tools.nrepl.middleware/descriptor {:requires #{#'session}
                                                               :expects #{#'clojure.tools.nrepl.middleware.pr-values/pr-values}})


(defn push-proxy [conn]
  (swap! *proxy-state* assoc :connection conn))

;; (defn pop-proxy []
;;   (swap! *proxy-state* assoc :pop? true))

(defn g [ports]
  (let [[port & ports] ports]
    (start-server
     :bind "127.0.0.1"
     :port port
     :handler (default-handler
                (cluster
                 ["127.0.0.1" port]
                 (fn [[host port]]
                   (repl/connect :host host :port port))
                 (for [port ports]
                   ["127.0.0.1" port]))
                #'proxy-state
                #'proxy-cutout))))

(defn f [& ports]
  (g
   (for [port ports]
     (Long/parseLong port))))


(defn h []
  (let [all-ports (range 10000 10050)]
    (future
      (g (take 1 all-ports)))
    (doseq [my-port (rest all-ports)]
      (Thread/sleep 100)
      (future
        (g [my-port (dec my-port)]))))
  @(promise))



(comment

  (in-ns 'com.manigfeald.crepl)
  (push-proxy (repl/connect :host "127.0.0.1" :port 10001))



  )
