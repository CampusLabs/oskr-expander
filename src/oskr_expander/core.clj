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

(ns oskr-expander.core
  (:require
    [com.stuartsierra.component :as component]
    [oskr-expander.producer :as producer]
    [oskr-expander.consumer :as consumer]
    [manifold.stream :as s]
    [oskr-expander.message :as m]
    [oskr-expander.process-manager :as process-manager]
    [cheshire.core :as json]
    [environ.core :refer [env]]))

(defn create-system []
  (-> (component/system-map
        :producer (producer/new-producer
                    (env "KAFKA_BOOTSTRAP" "kafka.service.consul:9092")
                    (env "KAFKA_TOPIC" "MessageParts")
                    :id)
        :process-manager (process-manager/new-process-manager)
        :consumer (consumer/new-consumer))

      (component/system-using
        {:process-manager [:consumer :producer]})))


(comment
  (def s (create-system)))

(comment
  (alter-var-root #'s component/start))

(comment
  (alter-var-root #'s component/stop))


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
        #(with-meta % {:topic     "MessageParts"
                       :partition (rand-int 8)
                       :offset    (swap! offset-atom inc)})
        specifications))

    (s/put-all! (get-in s [:consumer :message-stream]) (take 2 specifications-with-meta))))