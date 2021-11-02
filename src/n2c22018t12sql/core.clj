(ns n2c22018t12sql.core
  (:import [java.time LocalDate]
           [java.time.format DateTimeFormatter])
  (:require [clojure.data.xml :as xml]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [honey.sql :as sql]
            [honey.sql.helpers :as h]
            [next.jdbc :as jdbc]
            [cli-matic.core :refer [run-cmd]]))

(def normal-date-formatter
  "DateTimeFormatter for standard ISO 8601 yyyy-MM-dd dates."
  (DateTimeFormatter/ofPattern "yyyy-MM-dd"))

(def american-date-formatter
  "DateTimeFormatter for unofficial dates scraped from document 
   using the american MM/dd/yyyy date notation."
  (DateTimeFormatter/ofPattern "MM/dd/yyyy"))

(def pat-cols
  [[:pat-id :int [:primary-key]]])

(defn mk-doc-cols
  "Create column definitions for document table referencing
   the given patient table name as foreign key."
  [pat-table]
  [[:pat-id :int [:references pat-table :pat-id]]
   [:doc-id :int]
   [:date :date]
   [:text :text]
   [[:constraint :pat_doc] [:primary-key :pat-id :doc-id]]])

(defn mk-annotation-cols
  "Create column definitions for annotation table referencing
   the given patient table name as foreign key."
  [pat-table]
  [[:pat-id :int [:references pat-table :pat-id] [:primary-key]]
   [:abdominal :boolean]
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
   [:mi-6mos :boolean]])

(defn print-error
  "Print one or more strings to stderr."
  [& msgs]
  (binding [*out* *err*]
    (apply println msgs)))

(defn print-jdbc-help
  "Prints some examples of viable jdbc connection strings."
  []
  (print-error
   (str/join
    (System/lineSeparator)
    ["A JDBC connection string is a database-driver specific URL used to configure the connection."
     "Here are some common examples:"
     " jdbc:sqlite:test.db"
     " jdbc:sqlite:C:/SomeFolder/My.db"
     " jdbc:mariadb://localhost:3306/MyDBname?user=maria&password=hunter2"
     " jdbc:postgresql://localhost/n2c2?user=n2c2&password=badpassword"
     ""
     "The only drivers bundled with this app are sqlite, mariadb, and postgresql."])))

(defn lower-case-kw
  "Lowercase a keyword."
  [kw]
  (keyword (str/lower-case (name kw))))

(defn file->id
  "Given a file name, strip the file extension and return the base
   name e.g. \"100.xml\" -> \"100\"."
  [file]
  (let [file-name (.getName (io/file file))
        end-pos (if (.contains file-name ".")
                  (.indexOf file-name ".")
                  (dec (.length file-name)))]
    (Integer/valueOf (subs file-name 0 end-pos))))

(defn xml-file-seq
  "Given a directory return all XML files contained recursively within."
  [f]
  (filter #(str/ends-with? (str/lower-case (.getName %)) ".xml")
          (file-seq f)))

;; 100 stars
(def doc-separator (re-pattern (str/join "" (repeat 100 "\\*"))))

(defn get-and-split-texts
  "Given an n2c2 xml document tree, extract the text cdata and split into separate documents"
  [doc-tree]
  (-> doc-tree
      :content
      first
      :content
      first
      (str/split doc-separator)
      drop-last)) ;; each doc ends in separator, last item is single newline


(defn extract-date
  "Extracts the 'Record date: ' yyyy-MM-dd date, or a MM/dd/yy date (forced into 21st century)"
  [text]
  (let [normal-date (second (re-find (re-matcher #"(?i)Record date: ([0-9-]+)" text)))
        american-date (re-find (re-matcher #"([0-9][0-9]/[0-9][0-9]/)([0-9][0-9])" text))]
    (cond
      normal-date (LocalDate/parse normal-date normal-date-formatter)
      american-date (LocalDate/parse (str (second american-date)
                                          "20"
                                          (nth american-date 2))
                                     american-date-formatter)
      :else (LocalDate/of 3000 01 01))))

(defn extract-texts
  "Given an XML tree pull out the text CDATA section, split by the
   document separator (line of stars), assign an auto-incrementing
   ID to each document, extract the date found in it, and return
   a map of the patient ID, document ID, date, and text content."
  [doc-tree pat-id]
  (let [texts (get-and-split-texts doc-tree)]
    (map-indexed
     (fn [i text]
       {:doc-id i
        :pat-id pat-id
        :date (extract-date text)
        :text text})
     texts)))

(defn extract-tags
  "Given an XML tree pull out the annotation tags and insert them
   into a map with the tag name as a lowercase keyword."
  [doc-tree pat-id]
  (->> doc-tree
       :content
       second
       :content
       (reduce (fn [m elem]
                 (assoc m
                        (lower-case-kw (:tag elem))
                        (= (get-in elem [:attrs :met]) "met")))
               {:pat-id pat-id})))

(defn read-record
  "Given a file, parse it as XML and convert to a map of
   patient ID, annotations, and zero-indexed documents."
  [f annotated]
  (with-open [r (io/reader f)]
    (let [pat-id (file->id f)
          doc-tree (xml/parse r)
          docs (extract-texts doc-tree pat-id)
          tags (when annotated (extract-tags doc-tree pat-id))]
      {:pat-id pat-id
       :docs docs
       :annotations tags})))

(defn setup-tables!
  "Create (optionally dropping if exists) the patient, document,
   and annotation tables. The annotation tables will only be created
   if needed."
  [con annotated keep pat-table doc-table ann-table]
  (when-not keep
    (doseq [table [ann-table doc-table pat-table]]
      (jdbc/execute!
       con
       (sql/format {:drop-table [:if-exists table]}))))
  (jdbc/execute!
   con
   (sql/format {:create-table [pat-table :if-not-exists]
                :with-columns pat-cols}))
  (jdbc/execute!
   con
   (sql/format {:create-table [doc-table :if-not-exists]
                :with-columns (mk-doc-cols pat-table)}))
  (when annotated
    (jdbc/execute!
     con
     (sql/format {:create-table [ann-table :if-not-exists]
                  :with-columns (mk-annotation-cols pat-table)}))))

(defn import-data
  "Create tables, insert patients, insert documents, insert annotations."
  [{:keys [jdbc patient-table doc-table annotation-table dataset-dir annotated keep]}]
  (with-open [con (jdbc/get-connection {:jdbcUrl jdbc} {:auto-commit false})]
    (let [xml-files (xml-file-seq (io/file dataset-dir))
          records (map #(read-record % annotated) xml-files)]
      (setup-tables! con annotated keep patient-table doc-table annotation-table)
      (.commit con)

      ;; Insert patients
      (jdbc/execute!
       con
       (-> (h/insert-into patient-table)
           (h/values (map #(vector (:pat-id %)) records))
           (sql/format)))

      ;; Insert documents/texts
      (jdbc/execute!
       con
       (-> (h/insert-into doc-table)
           (h/values (flatten (map :docs records)))
           (sql/format)))
      
      ;; Insert annotations/cohort class match status
      (when annotated
        (jdbc/execute!
         con
         (-> (h/insert-into annotation-table)
             (h/values (flatten (map :annotations records)))
             (sql/format))))
      (.commit con))))

(defn try-import-data
  "Wrap the data import process in an error handler to 
   print more useful feedback to the user where possible."
  [args]
  (try
    (import-data args)
    (catch java.sql.SQLException e
      (print-error (.getMessage e))
      (cond
        (str/includes? (.getMessage e) "No suitable driver found")
          (print-jdbc-help)
        (str/includes? (.getMessage e) "duplicate key value")
          (print-error "Duplicate data created. Please do not re-append the same patients/documents into a database")
        :else
        (.printStackTrace e))
      (System/exit 1))))

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
          {:option "patient-table"
           :short "p"
           :default :patients
           :type :keyword
           :as "Table to insert patient IDs to"}
          {:option "doc-table"
           :short "t"
           :default :documents
           :type :keyword
           :as "Table to insert documents to"}
          {:option "annotation-table"
           :short "T"
           :default :annotations
           :type :keyword
           :as "Table to insert cohort annotations to"}
          {:option "annotated"
           :short "a"
           :default false
           :type :with-flag
           :as "Whether importing annotated (tagged) patients or not"}
          {:option "keep"
           :short "k"
           :default false
           :type :with-flag
           :as "Do not drop and re-create tables if they already exist"}
          {:option "dataset-dir"
           :short "d"
           :default :present
           :type :string
           :as "Directory containing dataset XML files"}]
   :runs try-import-data})

(defn -main
  [& args]
  (run-cmd args cli-configuration))