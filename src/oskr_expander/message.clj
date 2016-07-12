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

(ns oskr-expander.message
  (:require [oskr-expander.protocols :as p]))

(defrecord Punctuation
  [topic
   partition
   offset]

  p/Punctuated
  (punctuation? [_] true))

(defrecord Specification
  [id
   sentAt
   groupingKey
   senderId
   expansion
   tags
   immediate
   broadcast
   templates
   data]

  p/Punctuated
  (punctuation? [_] false))

(defrecord Part
  [id
   sentAt
   groupingKey
   senderId
   expansion
   tags
   immediate
   broadcast
   templates
   data
   digestAt
   channels
   recipientID]

  p/Punctuated
  (punctuation? [_] false))
