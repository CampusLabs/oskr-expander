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

(ns oskr-expander.specification
  (:require [com.stuartsierra.component :as component]
            [manifold.stream :as s]
            [oskr-expander.recipient :as r]
            [oskr-expander.protocols :as p]
            [oskr-expander.message :as m]
            [taoensso.timbre :refer [info debug error warn]]
            [cheshire.core :as json]))

(defn recipients->parts [{entity-data :data :as specification} recipients]
  (let [part-template (dissoc specification :expansion :data)]
    (map (fn [{:keys [id channels digestAt data]}]
           (->
             (assoc part-template :recipientId id
                                  :channels channels
                                  :digestAt digestAt
                                  :data {:recipient data
                                         :entity    entity-data})
             m/map->Part))
         recipients)))

(defn make-recipient-handler [specification parts-stream]
  (fn [recipients]
    (debug "enqueing parts")
    (->> (recipients->parts specification recipients)
         (s/put-all! parts-stream))))

(defn make-message-handler [parts-stream]
  (fn [message]
    (debug "handling messsage")
    (if (p/punctuation? message)
      (s/put! parts-stream message)
      (let [recipient-handler (make-recipient-handler message parts-stream)]
        (debug "expanding recipients")
        (r/expand recipient-handler (:expansion message))))))

(defn expand [specification-stream part-stream]
  (let [message-handler (make-message-handler part-stream)]
    (s/consume-async message-handler specification-stream)))

(defrecord Processor [part-stream specification-stream]
  component/Lifecycle
  (start [processor]
    (let [specification-stream (s/stream)]
      (expand specification-stream part-stream)
      (assoc processor :specification-stream specification-stream)))
  (stop [processor]
    (s/close! specification-stream)
    (assoc processor :specification-stream nil)))

(defn new-processor [part-stream]
  (Processor. part-stream nil))

(comment
  (do
    (def specification
      (-> "/Users/larslevie/workspace/oskr-expander/resources/message.json"
          (java.io.File.)
          slurp
          (json/parse-string true)
          :payload
          m/map->Specification))

    (def punctuation
      (m/map->Punctuation {:partitionId 1 :offset 1}))

    (def part-stream (s/stream))
    (def processor (new-processor part-stream))
    (alter-var-root #'processor component/start)
    (def parts (atom []))
    (s/consume #(swap! parts conj %) part-stream)
    (s/put! (:specification-stream processor) specification)
    (s/put! (:specification-stream processor) punctuation)
    (alter-var-root #'processor component/stop)))
