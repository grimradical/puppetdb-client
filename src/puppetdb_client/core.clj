(ns puppetdb-client.core
  (:require [clj-http.client :as client]
            [cheshire.core :as json]))

;; TODO:
;;
;; Replace "baseurl" in api-call with a proper map of params for using
;; the connection
;;
;; Need to serialize query parameters to JSON (especially "query")

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

(defn nodes
  ([db]
     (nodes db {}))
  ([db params]
     (api-call db "v3/nodes" params)))

(defn catalog
  [db node]
  (api-call db (format "v3/catalogs/%s" node) {}))

(defn server-time
  [db]
  (api-call db "v3/server-time" {}))

(defn version
  [db]
  (api-call db "v3/version" {}))

(defn metrics
  ([db]
     (api-call db "/v3/metrics/mbeans" {}))
  ([db metric]
     (api-call db (format "v3/metrics/mbean/%s" metric) {})))

(defn node-facts
  [& opts]
  (if (map? (last opts))
    (let [params (last opts)
          db     (first opts)
          args   (rest (butlast opts))
          n-args (count args)]
      (condp = n-args
        1
        (api-call db (format "v3/nodes/%s/facts" (first args)) params)

        2
        (api-call db (format "v3/nodes/%s/facts/%s" (first args) (second args)) params)

        3
        (api-call db (format "v3/nodes/%s/facts/%s/%s" (first args) (second args) (nth 2 args)) params)

        (throw (IllegalArgumentException. "Too many args"))))

    (apply node-facts (concat opts [{}]))))

(defn node-resources
  [& opts]
  (if (map? (last opts))
    (let [params (last opts)
          db     (first opts)
          args   (rest (butlast opts))
          n-args (count args)]
      (condp = n-args
        1
        (api-call db (format "v3/nodes/%s/resources" (first args)) params)

        2
        (api-call db (format "v3/nodes/%s/resources/%s" (first args) (second args)) params)

        3
        (api-call db (format "v3/nodes/%s/resources/%s/%s" (first args) (second args) (nth args 2)) params)

        (throw (IllegalArgumentException. "Too many args"))))

    (apply node-resources (concat opts [{}]))))

(defn facts
  [& opts]
  (if (map? (last opts))
    (let [params (last opts)
          db     (first opts)
          args   (rest (butlast opts))
          n-args (count args)]
      (condp = n-args
        0
        (api-call db (format "v3/facts" (first args)) params)

        1
        (api-call db (format "v3/facts/%s" (first args)) params)

        2
        (api-call db (format "v3/facts/%s/%s" (first args) (second args)) params)

        (throw (IllegalArgumentException. "Too many args"))))

    (apply facts (concat opts [{}]))))


(defn resources
  [& opts]
  (if (map? (last opts))
    (let [params (last opts)
          db     (first opts)
          args   (rest (butlast opts))
          n-args (count args)]
      (condp = n-args
        0
        (api-call db (format "v3/resources" (first args)) params)

        1
        (api-call db (format "v3/resources/%s" (first args)) params)

        2
        (api-call db (format "v3/resources/%s/%s" (first args) (second args)) params)

        (throw (IllegalArgumentException. "Too many args"))))

    (apply resources (concat opts [{}]))))

(defn reports
  [db params]
  (api-call db "/v3/reports" params))

(defn events
  [db params]
  (api-call db "/v3/events" params))

(defn event-counts
  [db params]
  (api-call db "/v3/event-counts" params))

(defn aggregate-event-counts
  [db params]
  (api-call db "/v3/aggregate-event-counts" params))
