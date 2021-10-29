(ns n2c22018t12sql.core
  (:require [clojure.data.xml :as xml]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [honey.sql :as sql]
            [honey.sql.helpers :as h]
            [next.jdbc :as jdbc]
            [cli-matic.core :refer [run-cmd]]))

(def core-cols
  [[:doc-id :int]
   [:text :text]])

(def annotated-cols
  (concat
   core-cols
   [[:abdominal :boolean]
    [:advanced_cad :boolean]
    [:alcohol-abuse :boolean]
    [:asp-for-mi :boolean]
    [:creatinine :boolean]
    [:dietsupp-2mos :boolean]
    [:drug-abuse :boolean]
    [:english :boolean]
    [:hba1c :boolean]
    [:keto-1yr :boolean]
    [:major-diabetes :boolean]
    [:makes-decisions :boolean]
    [:mi-6mos :boolean]]))

(defn lower-case-kw
  [kw]
  (keyword (str/lower-case (name kw))))

(defn file->id
  [file]
  (let [file-name (.getName (io/file file))
        end-pos (if (.contains file-name ".")
                  (.indexOf file-name ".")
                  (dec (.length file-name)))]
    (Integer/valueOf (subs file-name 0 end-pos))))

(defn xml-file-seq
  [f]
  (filter #(str/ends-with? (.getName %) ".xml")
          (file-seq f)))

(defn read-n2c2-xml
  [f]
  (with-open [r (io/reader f)]
    (-> (xml/parse r)
        :content
        first
        :content
        first)))

(defn extract-text
  [doc-tree]
  (-> doc-tree
      :content
      first
      :content
      first))

(defn extract-tags
  [doc-tree]
  (->> doc-tree
       :content
       second
       :content
       (reduce (fn [m elem]
                 (assoc m 
                        (lower-case-kw (:tag elem))
                        (= (get-in elem [:attrs :met]) "met")))
               {})))

(defn read-record
  [f annotated]
  (with-open [r (io/reader f)]
    (let [doc-id (file->id f)
          doc-tree (xml/parse r)
          text (extract-text doc-tree)
          tags (when annotated (extract-tags doc-tree))]
      (merge {:doc-id doc-id
              :text text} 
             tags))))

(defn import-data
  [{:keys [jdbc table-name dataset-dir annotated]}]
  (with-open [con (jdbc/get-connection {:jdbcUrl jdbc} {:auto-commit false})]
    (let [cols (if annotated annotated-cols core-cols)
          xml-files (xml-file-seq (io/file dataset-dir))
          records (map #(read-record % annotated) xml-files)]
      (jdbc/execute!
       con
       (sql/format {:drop-table [:if-exists table-name]}))
      (jdbc/execute!
       con
       (sql/format {:create-table [table-name :if-not-exists]
                    :with-columns cols}))
      (.commit con)
      (jdbc/execute!
       con
       (-> (h/insert-into table-name)
           (h/values records)
           (sql/format {:pretty true})))
      (.commit con))))

  (def cli-configuration
    {:command "n2c22018t12sql"
     :description "Import n2c2 \"Track 1 Cohort Selection for Clinical Trials\" data from XML into SQL"
     :version "1.0.0"
     :opts [{:option "jdbc"
             :short "j"
             :default :present
             :env "JDBC"
             :type :string
             :as "JDBC DSN connection string"}
            {:option "table-name"
             :short "t"
             :default :present
             :type :keyword
             :as "Table to insert records to"}
            {:option "annotated"
             :short "a"
             :default false
             :type :flag}
            {:option "dataset-dir"
             :short "d"
             :default :present
             :type :string
             :as "Directory containing dataset XML files"}]
     :runs import-data})

(defn -main
  [& args]
  (run-cmd args cli-configuration))