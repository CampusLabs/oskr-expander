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

(ns oskr-expander.process-manager
  (:require [com.stuartsierra.component :as component]
            [manifold.stream :as s]
            [clojure.tools.logging :refer [info debug error warn]]
            [oskr-expander.processor :as spec]
            [oskr-expander.message :as m]
            [oskr-expander.producer :as producer]
            [cheshire.core :as json]))

(defn enqueue-specification [{:keys [specification-stream]} specification]
  (info "enqueing specification onto processor")
  (s/put! specification-stream specification))

(defn route-specification [specification producer consumer processor-atom]
  (info "routing specification")
  (info processor-atom)
  (let [partition-key (-> (meta specification)
                          (select-keys [:topic :partition]))
        processors @processor-atom]
    (println partition-key processors)
    (if-let [processor (processors partition-key)]
      (enqueue-specification processor specification)
      (let [processor (-> (spec/new-processor producer consumer)
                          component/start)]
        (swap! processor-atom #(assoc % partition-key processor))
        (enqueue-specification processor specification)))))

(defn consume [consumer producer processor-atom]
  (info "consuming specification stream")
  (s/consume-async
    #(route-specification % producer consumer processor-atom)
    (:message-stream consumer)))

(defrecord ProcessManager [consumer producer processor-atom]
  component/Lifecycle
  (start [process-manager]
    (info "starting process mananger")
    (let [processor-atom (atom {})]
      (consume consumer producer processor-atom)
      (assoc process-manager :processor-atom processor-atom)))
  (stop [process-manager]
    (info "stopping process mananger")
    (doall (pmap component/stop (vals @processor-atom)))
    (assoc process-manager :processor-atom nil)))

(defn new-process-manager []
  (ProcessManager. nil nil nil))

(comment
  (do
    (s/put-all! specification-stream (take 12 specifications-with-meta))))


(comment
  (do
    (def offset-atom (atom 0))
    (def specifications
      (-> "/code/resources/message.json"
          (java.io.File.)
          slurp
          (json/parse-string true)
          :payload
          m/map->Specification
          repeat))

    (def specifications-with-meta
      (map
        #(with-meta % {:topic     "Test"
                       :partition (rand-int 8)
                       :offset    (swap! offset-atom inc)})
        specifications))

    (def specification-stream (s/stream))

    (def producer (producer/new-producer "kafka.service.consul:9092" "MessageParts" :id))
    (alter-var-root #'producer component/start)
    (def process-manager (-> (new-process-manager specification-stream)
                             (assoc :producer producer)))
    (alter-var-root #'process-manager component/start)))

(comment
  (do
    (alter-var-root #'process-manager component/stop)
    (alter-var-root #'producer component/stop)))
