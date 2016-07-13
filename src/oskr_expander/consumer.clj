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
            [manifold.stream :as s]
            [taoensso.timbre :refer [info debug error warn]]))

(defrecord Consumer [message-stream]
  component/Lifecycle
  (start [consumer]
    (debug "Starting consumer")
    (let [message-stream (s/stream)]
      (assoc consumer :message-stream message-stream)))
  (stop [consumer]
    (debug "Stopping consumer")
    (s/close! message-stream)
    (assoc consumer :message-stream nil)))

(defn new-consumer [] (Consumer. nil))