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

(ns oskr-expander.producer
  (:require [com.stuartsierra.component :as component]
            [oskr-expander.protocols :as p]
            [clojure.walk :refer [stringify-keys]]
            [cheshire.core :as json]
            [manifold.deferred :as d]
            [taoensso.timbre :refer [info debug error warn]])
  (:import [org.apache.kafka.common.serialization StringSerializer]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [java.util Map]
           [java.util.concurrent TimeUnit]))

(defrecord Producer [config topic key-fn kafka-producer]
  p/Publishable
  (send-message [_ message]
    (let [key (key-fn message)
          message-string (json/generate-string message)
          record (ProducerRecord. topic key message-string)]
      (debug record)
      (.send kafka-producer record)))

  (flush-messages [_]
    (.flush kafka-producer))

  component/Lifecycle
  (start [producer]
    (let [kafka-producer (KafkaProducer. ^Map config)]
      (assoc producer :kafka-producer kafka-producer)))

  (stop [producer]
    (.close kafka-producer 10 TimeUnit/SECONDS)
    (assoc producer :kafka-producer nil)))

(defn new-producer [kafka-bootstrap topic key-fn]
  (Producer. (-> {:bootstrap.servers kafka-bootstrap
                  :key.serializer    StringSerializer
                  :value.serializer  StringSerializer
                  :acks              "all"
                  :compression.type  "gzip"
                  :retries           (int 2147483647)}
                 stringify-keys)
             topic key-fn nil))

(comment
  (do
    (def key-fn :id)
    (def producer (new-producer "kafka.service.consul:9092" "MessageParts" key-fn))
    (alter-var-root #'producer component/start)))