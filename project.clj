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

(defproject oskr-expander "0.1.0"
  :description "A recipient expander companion service for oskr-events."
  :url "http://github.com/orgsync/oskr-expander"
  :license {:name "Apache 2.0"
            :url  "http://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[aleph "0.4.1"]
                 [org.clojure/clojure "1.8.0"]
                 [byte-streams "0.2.2"]
                 [org.clojure/tools.logging "0.3.1"]
                 [cheshire "5.6.1"]
                 [com.stuartsierra/component "0.3.1"]
                 [org.apache.kafka/kafka-clients "0.9.0.1"]
                 [org.bovinegenius/exploding-fish "0.3.4"]
                 [manifold "0.1.5-alpha2"]
                 [org.apache.kafka/kafka-clients "0.9.0.1"]
                 [environ "1.0.3"]
                 [org.slf4j/slf4j-log4j12 "1.7.13"]
                 [org.slf4j/slf4j-api "1.7.13"]
                 [log4j/log4j "1.2.17"]]
  :profiles {:repl {:jvm-opts
                    ["-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"]}})
