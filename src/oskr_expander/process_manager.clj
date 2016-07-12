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
            [taoensso.timbre :refer [info debug error warn]]
            [oskr-expander.specification :as spec]
            [oskr-expander.message :as m]
            [cheshire.core :as json]))

(defn enqueue-specification [{:keys [specification-stream]} specification]
  (debug "enqueing specification onto processor")
  (s/put! specification-stream specification))

(defn route-specification [specification part-stream processor-atom]
  (debug "routing specification")
  (debug processor-atom)
  (let [partition-key (-> (meta specification)
                          (select-keys [:topic :partition]))
        processors @processor-atom]
    (println partition-key processors)
    (if-let [processor (processors partition-key)]
      (enqueue-specification processor specification)
      (let [processor (-> (spec/new-processor part-stream)
                          component/start)]
        (swap! processor-atom #(assoc % partition-key processor))
        (enqueue-specification processor specification)))))

(defn consume [specification-stream part-stream processor-atom]
  (debug "consuming specification stream")
  (s/consume-async
    #(route-specification % part-stream processor-atom)
    specification-stream))

(defrecord ProcessManager [specification-stream part-stream processor-atom]
  component/Lifecycle
  (start [process-manager]
    (debug "starting process mananger")
    (let [processor-atom (atom {})
          part-stream (s/stream)]
      (consume specification-stream part-stream processor-atom)
      (assoc process-manager
        :processor-atom processor-atom
        :part-stream part-stream))
    )
  (stop [process-manager]
    (debug "stopping process mananger")
    (doall (pmap component/stop (vals @processor-atom)))
    (s/close! part-stream)
    (assoc process-manager :part-stream nil :processor-atom nil)))

(defn new-process-manager [specification-stream]
  (ProcessManager. specification-stream nil nil))

(comment
  (do
    (def offset-atom (atom 0))
    (def specifications
      (-> "/Users/larslevie/workspace/oskr-expander/resources/message.json"
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
    (def process-manager (new-process-manager specification-stream))
    (alter-var-root #'process-manager component/start)
    (def part-stream (:part-stream process-manager))
    (def parts (atom []))
    (s/consume #(swap! parts conj %) part-stream)
    (s/put-all! specification-stream (take 12 specifications-with-meta))
    (alter-var-root #'process-manager component/stop)))