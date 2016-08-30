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

(ns oskr-expander.processor
  (:require [com.stuartsierra.component :as component]
            [manifold.stream :as s]
            [manifold.deferred :as d]
            [oskr-expander.recipient :as r]
            [oskr-expander.message :as m]
            [oskr-expander.protocols :as p]
            [clojure.tools.logging :refer [info debug error warn]]
            [cheshire.core :as json]))

(defn recipients->part [{id :id entity-data :data :as specification} recipients]
  (let [part-template (dissoc specification :id :expansion :data)
        recipients-data (map (fn [{:keys [id channels digest data]}]
                               {:id       id
                                :channels channels
                                :digest   digest
                                :data     data}),
                             recipients)]

    (-> (assoc part-template :specificationId id
                             :recipients recipients-data
                             :data entity-data)
        m/map->Part
        (with-meta (meta specification)))))

(defn make-recipient-handler [specification producer]
  (fn [recipients]
    (info "enqueing part")
    (let [part (recipients->part specification recipients)]
      (info "part ->" part)
      (p/send-message! producer part))))

(defn make-message-handler [producer consumer]
  (fn [specification]
    (info "handling messsage")

    (let [recipient-handler (make-recipient-handler specification producer)]
      (info "expanding recipients" (meta specification))

      (d/chain' (r/expand recipient-handler (:expansion specification))
        (fn [_]
          (p/flush-messages producer)
          (p/commit! consumer (meta specification)))))))

(defn expand [specification-stream producer consumer]
  (let [message-handler (make-message-handler producer consumer)]
    (d/loop []
      (d/let-flow [specification (s/take! specification-stream)]
        (when specification
          (d/chain' (message-handler specification)
            (fn [_] (d/recur))))))))

(defrecord Processor [producer consumer specification-stream finished?]
  component/Lifecycle
  (start [processor]
    (info "Starting processor")

    (let [specification-stream (s/stream 16)
          finished? (expand specification-stream producer consumer)]
      (assoc processor :specification-stream specification-stream :finished? finished?)))

  (stop [processor]
    (info "Stopping processor")
    (s/close! specification-stream)
    @finished?
    (assoc processor :specification-stream nil :finished? nil)))

(defn new-processor [producer consumer]
  (Processor. producer consumer nil nil))

(comment
  (do
    (def specification
      (-> "/code/resources/message.json"
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
