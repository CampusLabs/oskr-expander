;
; Copyright 2016 OrgSync.
;
; Licensed under the Apache License, Version 2.0 (the "License")
; you may not use this file except in compliance with the License.
; You may obtain a copy of the License at
;
;   http://www.apache.org/licenses/LICENSE-2.0
;
; Unless required by applicable law or agreed to in writing, software
; distributed under the License is distributed on an "AS IS" BASIS,
; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
; See the License for the specific language governing permissions and
; limitations under the License.
;

(ns oskr-expander.consumer
  (:require [com.stuartsierra.component :as component]
            [byte-streams :as byte]
            [manifold.stream :as s]
            [oskr-expander.protocols :as p]
            [clojure.walk :refer [stringify-keys]]
            [clojure.tools.logging :refer [info debug error warn]]
            [cheshire.core :as json])
  (:import [org.apache.kafka.common.serialization ByteArrayDeserializer StringDeserializer]
           [org.apache.kafka.clients.consumer KafkaConsumer ConsumerRecord OffsetAndMetadata OffsetCommitCallback]
           [java.util Map]
           [java.io Reader]
           [org.apache.kafka.common TopicPartition]))

(defn record->specification [^ConsumerRecord record]
  (-> (.value record)
      (byte/convert Reader)
      (json/parse-stream true)
      :payload
      (with-meta {:topic     (.topic record)
                  :partition (.partition record)
                  :offset    (.offset record)
                  :key       (.key record)})))

(defn poll-fn [kafka-consumer message-stream]
  (info "defining poller")
  (fn []
    (when-not (s/closed? message-stream)
      (info "polling")
      (->> (try (locking kafka-consumer
                  (.poll kafka-consumer 1000))
                (catch Exception e
                  (warn "polling failed" (.getMessage e))))
           (.iterator)
           (iterator-seq)
           (s/put-all! message-stream)
           deref)
      (recur))))

(defn spawn-poller [kafka-consumer message-stream]
  (info "spawning poller")
  (->
    (Thread. ^Runnable (poll-fn kafka-consumer message-stream) "kafka-consumer")
    (.start)))

(defrecord Consumer [config topic message-stream kafka-consumer]
  component/Lifecycle
  (start [consumer]
    (info "Starting consumer")
    (info "kafka-consumer" config)

    (let [message-stream (s/stream* {:xform (map record->specification)})
          kafka-consumer (try (KafkaConsumer. ^Map config)
                              (catch Exception e (error e)))]
      (spawn-poller kafka-consumer message-stream)
      (locking kafka-consumer (.subscribe kafka-consumer [topic]))

      (assoc consumer :message-stream message-stream :kafka-consumer kafka-consumer)))

  (stop [consumer]
    (info "Stopping consumer")
    (s/close! message-stream)
    (locking kafka-consumer (.close kafka-consumer))

    (assoc consumer :message-stream nil :kafka-consumer nil))

  p/Commitable
  (commit! [_ {:keys [partition offset] :as m}]
    (info "acking to consumer" m)
    (let [topic-partition (TopicPartition. (:topic m) partition)
          offset-and-metadata (OffsetAndMetadata. (inc offset))
          offsets {topic-partition offset-and-metadata}
          callback (reify OffsetCommitCallback
                     (onComplete [_ _ e]
                       (when e
                         (warn "Unable to commit" m (.getMessage e)))))]

      (locking kafka-consumer
        (.commitAsync kafka-consumer offsets callback)))))

(defn new-consumer [kafka-bootstrap group-id topic]
  (Consumer. (-> {:bootstrap.servers  kafka-bootstrap
                  :key.deserializer   StringDeserializer
                  :value.deserializer ByteArrayDeserializer
                  :group.id           group-id
                  :enable.auto.commit false}
                 stringify-keys)
             topic nil nil))

(comment
  (do
    (let [message (-> "/code/resources/message.json"
                      (java.io.File.)
                      slurp
                      (.getBytes))
          record (ConsumerRecord. "MessageParts" 1 1 "iosdyfu938" message)]
      (record->specification record))))
