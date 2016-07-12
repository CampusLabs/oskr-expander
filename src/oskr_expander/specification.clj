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
            [manifold.deferred :as d]
            [oskr-expander.recipient :as r]
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
             m/map->Part
             (with-meta (meta specification))))
         recipients)))

(defn make-recipient-handler [specification parts-stream]
  (fn [recipients]
    (debug "enqueing parts")
    (->> (recipients->parts specification recipients)
         (s/put-all! parts-stream))))

(defn make-message-handler [part-stream]
  (fn [specification]
    (debug "handling messsage")
    (let [recipient-handler (make-recipient-handler specification part-stream)
          punctuation (m/map->Punctuation (meta specification))]
      (debug "expanding recipients" (meta specification))
      (d/chain' (r/expand recipient-handler (:expansion specification))
        (fn [_]
          (debug "punctuation" (meta specification))
          (s/put! part-stream punctuation))))))

(defn expand [specification-stream part-stream]
  (let [message-handler (make-message-handler part-stream)]
    (d/loop []
      (d/let-flow [specification (s/take! specification-stream)]
        (when specification
          (d/chain' (message-handler specification)
            (fn [_] (d/recur))))))))

(defrecord Processor [part-stream specification-stream finished?]
  component/Lifecycle
  (start [processor]
    (let [specification-stream (s/stream 16)
          finished? (expand specification-stream part-stream)]
      (assoc processor :specification-stream specification-stream :finished? finished?)))
  (stop [processor]
    (s/close! specification-stream)
    @finished?
    (assoc processor :specification-stream nil :finished? nil)))

(defn new-processor [part-stream]
  (Processor. part-stream nil nil))

(comment
  (do
    (def specification
      (-> "/Users/larslevie/workspace/oskr-expander/resources/message.json"
          (java.io.File.)
          slurp
          (json/parse-string true)
          :payload
          m/map->Specification))

    (def part-stream (s/stream))
    (def processor (new-processor part-stream))
    (alter-var-root #'processor component/start)
    (def parts (atom []))
    (s/consume-async #(swap! parts conj %) part-stream)
    (s/put! (:specification-stream processor) specification)
    (alter-var-root #'processor component/stop)))
