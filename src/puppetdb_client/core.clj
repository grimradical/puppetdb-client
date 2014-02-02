(ns puppetdb-client.core
  (:require [clj-http.client :as client]
            [cheshire.core :as json]))

;; TODO:
;;
;; Replace "baseurl" in api-call with a proper map of params for using
;; the connection
;;
;; Need to serialize query parameters to JSON (especially "query")
;;
;; Need connection timout and total timeout (possible?)

(defn serialize-query-param
  [params]
  (if (params :query)
    (update-in params [:query] json/generate-string)
    params))

(defn api-call [baseurl path params]
  (let [response (client/get (str baseurl "/" path)
                             {:query-params (serialize-query-param params)
                              :as           :json})]
    (:body response)))

(defn api-func
  [urls]
  (let [url-map (into {} (for [url urls]
                           [(count (re-seq #"%s" url)) url]))]
    (fn [& opts]
      (if (map? (last opts))
        (let [params  (last opts)
              db      (first opts)
              args    (rest (butlast opts))
              n-args  (count args)
              url     (url-map n-args)]
          (api-call db (apply format url args) params))
        (recur (concat opts [{}]))))))

(def nodes
  (api-func ["v3/nodes"
             "v3/nodes/%s"]))

(def metrics
  (api-func ["/v3/metrics/mbeans"
             "/v3/metrics/mbean/%s"]))

(def node-facts
  (api-func ["v3/nodes/%s/facts"
             "v3/nodes/%s/facts/%s"
             "v3/nodes/%s/facts/%s/%s"]))

(def node-resources
  (api-func ["v3/nodes/%s/resources"
             "v3/nodes/%s/resources/%s"
             "v3/nodes/%s/resources/%s/%s"]))

(def facts
  (api-func ["v3/facts"
             "v3/facts/%s"
             "v3/facts/%s/%s"]))

(def resources
  (api-func ["v3/resources"
             "v3/resources/%s"
             "v3/resources/%s/%s"]))

(def catalog (api-func ["v3/catalogs/%s"]))

(def server-time (api-func ["v3/server-time"]))

(def version (api-func ["v3/version"]))

(def reports (api-func ["v3/reports"]))

(def events (api-func ["v3/reports"]))

(def event-counts (api-func ["v3/reports"]))

(def aggregate-event-counts (api-func ["v3/reports"]))
