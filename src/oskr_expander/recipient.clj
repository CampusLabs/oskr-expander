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
            [clojure.tools.logging :refer [info debug error warn trace]])
  (:import (java.io Reader)
           [clojure.lang ExceptionInfo]
           [java.util.concurrent TimeoutException]))

(def ^:const retry-limit 7)

(defn build-url [url-base bulk-id]
  (cond-> (uri/uri url-base)
          bulk-id (uri/param "bulk_id" bulk-id)
          true str))

(defn parse-body [{:keys [body]}]
  (info "parsing request body")
  (-> (byte/convert body Reader)
      (json/parse-stream true)))

(defn exp-backoff-time [tries]
  (min (/ (dec (bit-shift-left 1 tries)) 2/1000) 300000))

(defn retry [tries last-response]
  (if (< tries (inc retry-limit))
    (do (info "queuing request for retry")
        (d/chain' (d/timeout! (d/deferred) (exp-backoff-time tries) true)
          (fn [_] (d/recur (inc tries)))))

    (do (warn "retry limit reached; request failed permanently")
        last-response)))

(defn external-failure? [{:keys [status]}]
  (if status
    (or (>= status 500) (== status 429))
    true))

(defn bad-request? [{:keys [status]}]
  (when status (and (<= 400 status 499) (not= status 429))))

(defn execute-query [url body]
  (info "requesting url" url)

  (d/loop [tries 1]
    (info "looping" tries)

    (-> (d/chain'
          (http/get url {:headers {:content-type "application/json"}
                         :body    body})
          (fn [response] response))

        (d/timeout! 30000)

        (d/catch' ExceptionInfo
          (fn [e]
            (let [response (ex-data e)]
              (cond
                (external-failure? response)
                (do
                  (warn "request failed:" (.getMessage e))
                  (retry tries response))

                (bad-request? response)
                (let [request-string (when body (String. body))
                      parsed-response (parse-body response)]
                  (warn ":" (.getMessage e))
                  (warn {:request  request-string
                         :response parsed-response}))

                :else
                (do (warn "request failed permanently:" (.getMessage e))
                    response)))))

        (d/catch' TimeoutException
          (fn [e]
            (warn "request timed out:" (.getMessage e))
            (retry tries nil)))

        (d/catch' Exception
          (fn [e]
            (warn "request failed permanently:" (.getMessage e))
            nil)))))

(defn expand [process-recipients {:keys [endpoint] :as expansion}]
  (info "generate query")
  (let [query-json (json/generate-string expansion)]
    (d/loop [bulk-id nil]
      (-> (d/let-flow [url (build-url endpoint bulk-id)
                       response (execute-query url query-json)]
            (when response
              (d/let-flow [{:keys [bulk_id recipients]} (parse-body response)
                           success? (process-recipients recipients)]
                (if (and bulk_id success?)
                  (d/recur bulk_id)
                  success?))))
          (d/catch' (fn [e] (error e)))))))
