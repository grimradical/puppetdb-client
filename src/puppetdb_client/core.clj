(ns puppetdb-client.core
  (:require [clj-http.client :as client]
            [cheshire.core :as json]))

;; TODO:
;;
;; Replace "baseurl" in api-call with a proper map of params for using
;; the connection
;;
;; HTTPS support (client certs, etc)

(defn serialize-query-param
  "In the parameter map, serialize the values associated with the query
  keyword to proper JSON encoded data."
  [params]
  (if (params :query)
    (update-in params [:query] json/generate-string)
    params))

(defn api-call [baseurl path params]
  "Query the API. This requires a baseurl (protocol://host:port) where to
  find PuppetDB, the path/endpoint to query including the API version and
  query parameters."
  (let [conn-params {:connect-timeout 10
                     :socket-timeout  60
                     :as              :json
                     :query-params    (serialize-query-param params)}
        response    (client/get (str baseurl "/" path) conn-params)]
    (:body response)))

(defn api-func
  "Takes a vector of paths with substitution markers %s. Returns a function
  that based on the amount of arguments figures out which part of the endpoint
  to query. If the last argument to the returned function is a map it is
  passed as the params argument to api-call."
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
  "Query the nodes endpoint for nodes. Without a certname it will return all
  the active nodes in PuppetDB."
  (api-func ["v3/nodes"
             "v3/nodes/%s"]))

(def node-facts
  "Query for facts of a specific node. Requires at least a certname to work
  with and optionally a fact name and possibly value to look for."
  (api-func ["v3/nodes/%s/facts"
             "v3/nodes/%s/facts/%s"
             "v3/nodes/%s/facts/%s/%s"]))

(def node-resources
  "Query for resources of a specific node. Requires at least a certname to work
  with and optionally the type and possibly the title of the resource."
  (api-func ["v3/nodes/%s/resources"
             "v3/nodes/%s/resources/%s"
             "v3/nodes/%s/resources/%s/%s"]))

(def metrics
  "Query the metrics endpoint. Without a metric name it will return the names
  of all the possible metrics."
  (api-func ["/v3/metrics/mbeans"
             "/v3/metrics/mbean/%s"]))

(def facts
  "Query the facts endpoint. Without a fact name it will return all facts
  stored in PuppetDB. It can optionally take a fact name and possibly a value
  to look for."
  (api-func ["v3/facts"
             "v3/facts/%s"
             "v3/facts/%s/%s"]))

(def resources
  "Query the resources endpoint. Without a resource type it will return all
  resources stored in PuppetDB; doing so will likely mean you'll hit the
  resource query limit. It can optionally take a resource type and possibly
  a title to look for."
  (api-func ["v3/resources"
             "v3/resources/%s"
             "v3/resources/%s/%s"]))

(def catalog 
  "Query the catalog endpoint. This requires a certname to be passed in."
  (api-func ["v3/catalogs/%s"]))

(def server-time
  "Query the server-time endpoint, returns the time on the server PuppetDB is
  running on."
  (api-func ["v3/server-time"]))

(def version
  "Query the version endpoint, returns the PuppetDB version."
  (api-func ["v3/version"]))

(def reports
  "Query the reports endpoint. This endpoint requires a query to be passed in
  scoping the request to either the certname or a report hash."
  (api-func ["v3/reports"]))

(def events
  "Query the events endpoint. This endpoint requires a query scoping the 
  request."
  (api-func ["v3/events"]))

(def event-counts
  "Query the event-counts endpoint. This endpoint requires a query scoping the
  request."
  (api-func ["v3/event-counts"]))

(def aggregate-event-counts
  "Query the aggregate-event-counts endpoint. This endpoint requires a query
  scoping the request."
  (api-func ["v3/aggregate-event-counts"]))
