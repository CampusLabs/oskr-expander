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

(ns oskr-expander.recipient
  (:require [aleph.http :as http]
            [byte-streams :as byte]
            [manifold.deferred :as d]
            [cheshire.core :as json]
            [org.bovinegenius.exploding-fish :as uri]
            [taoensso.timbre :refer [info debug error warn]])
  (:import (java.io Reader)))

(defn build-url [url-base bulk-id]
  (cond-> (uri/uri url-base)
          bulk-id (uri/param "bulk_id" bulk-id)
          true str))

(defn execute-query [url body]
  (debug "requesting url" url)
  (http/get url {:headers {:content-type "application/json"}
                 :body body}))

(defn parse-body [{:keys [body]}]
  (debug "parsing request body")
  (-> (byte/convert body Reader)
      (json/parse-stream true)))

(defn expand [process-recipients {:keys [endpoint] :as expansion}]
  (debug "generate query")
  (let [query-json (json/generate-string expansion)]
    (d/loop [bulk-id nil]
      (d/let-flow [url (build-url endpoint bulk-id)
                   response (execute-query url query-json)
                   {:keys [bulk_id recipients]} (parse-body response)
                   success? (process-recipients recipients)]
        (if (and bulk_id success?)
          (d/recur bulk_id)
          (do
            (debug "Done")
            success?))))))
