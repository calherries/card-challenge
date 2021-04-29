(ns card-challenge.core
  (:require [next.jdbc :as jdbc]
            [next.jdbc.sql :as sql]
            [next.jdbc.result-set :as rs]
            [clojure.set :as set]
            [clojure.pprint :as pp]
            [camel-snake-kebab.core :as csk]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io])
  (:import (java.sql ResultSet ResultSetMetaData)))

(def suits #{"hearts" "clubs" "diamonds" "spades"})

(def short->long-suit-name
  {"HE" "hearts"
   "CL" "clubs"
   "DI" "diamonds"
   "SP" "spades"})

(def long->short-suit-name
  (set/map-invert short->long-suit-name))

(def ranks #{"A" "0" "1" "2" "3" "4" "5" "6" "7" "8" "9" "10" "J" "Q" "K"})

(defn short-name
  "Short name of a card, based on it's suit and rank"
  [{:keys [suit rank]}]
  (str rank " " (long->short-suit-name suit)))

(defn long-name
  "Long name of a card, based on it's suit and rank"
  [{:keys [suit rank]}]
  (str rank "of" suit))

(def card-deck
  (for [suit suits
        rank ranks]
    (short-name {:suit suit :rank rank})))

(defn select-cards
  "Performs one selection of 10 cards from the remaining deck."
  [{:keys [selected-cards remaining-cards]}]
  (let [new-selected-cards (take 10 (shuffle remaining-cards))]
    {:selected-cards (concat selected-cards
                             new-selected-cards)
     :remaining-cards (remove #(some #{%} new-selected-cards)
                              remaining-cards)}))

(defn initial-state
  "Initialises the state of the game from the card deck.
   Some cards are selected, and the rest are remaining."
  [card-deck]
  {:selected-cards  []
   :remaining-cards card-deck})

;;; For displaying the states of the deck
(defn short-name->long-name
  [short-name]
  (str (re-find #"[^\s]+" short-name)
       " of "
       (short->long-suit-name (re-find #"[^\s]+$" short-name))))

(comment
  (= "9 of hearts"
     (short-name->long-name "9 HE")))

(defn print-deck-state
  [{:keys [selected-cards remaining-cards]}]
  (println (clojure.string/join \newline
                                (flatten ["Selected cards:"
                                          (map short-name->long-name selected-cards)
                                          "Remaining cards:"
                                          (map short-name->long-name remaining-cards)]))))


(def db
  (atom nil))

(defn start-db!
  []
  (reset! db (jdbc/get-datasource {:dbtype "sqlite" :dbname "card_deck_db"}))
  @db)

(defn stringify-keywords
  [m]
  (reduce-kv (fn [m k v]
               (assoc m
                      (csk/->snake_case (name k))
                      (if (keyword? v)
                        (csk/->snake_case (name v))
                        v)))
             {}
             m))

;; for example
(comment
  (= (stringify-keywords
      {:kebab-cased-key :kebab-cased-value})
     {"kebab_cased_key" "kebab_cased_value"}))

(defn write-card-rows-to-db!
  "Writes a list of card maps directly to the card table."
  [cards]
  (try
    (jdbc/execute-one! @db ["drop table if exists card"])
    (jdbc/execute-one! @db ["
create table card (
  id               integer primary key autoincrement,
  short_name       varchar(32),
  order_index      integer,
  is_selected      boolean
)"])
    (doseq [card cards]
      (sql/insert! @db :card card))
    (println "Populated database with initial data.")
    (catch Exception e
      (println "Exception:" (ex-message e))
      (println "Unable to populate the initial data."))))

(defn state->rows
  "Transforms the state map into a list of flat maps (rows) to persist to a db, or csv"
  [{:keys [selected-cards remaining-cards]}]
  (concat
   (map-indexed (fn [idx card] {:short-name card :order-index idx :is-selected true}) selected-cards)
   (map-indexed (fn [idx card] {:short-name card :order-index idx :is-selected false}) remaining-cards)))

(defn state->db-rows
  "Transforms the state map into rows for persisting directly to the db"
  [state]
  (->> state
       state->rows
       (map stringify-keywords)))

;; Quick test
(comment
  (let [csv-rows (state->db-rows {:selected-cards  ["Q of SP"]
                                  :remaining-cards ["K of CL"]})]
    (set csv-rows)))

(defn execute!
  "Executes a sql statement with a ResultSet builder that converts
   case and booleans back to standard Clojure format"
  [db sql]
  (jdbc/execute! db
                 sql
                 {:builder-fn (rs/builder-adapter
                               rs/as-unqualified-kebab-maps
                               (fn [builder ^ResultSet rs ^Integer i]
                                 (let [rsm ^ResultSetMetaData (:rsmeta builder)]
                                   (rs/read-column-by-index
                                    (if (#{"BIT" "BOOL" "BOOLEAN"} (.getColumnTypeName rsm i))
                                      (.getBoolean rs i)
                                      (.getObject rs i))
                                    rsm
                                    i))))}))

(defn read-db-rows
  "Reads all the cards from the database"
  []
  (set (execute! @db ["select * from card"])))

;; Now, for writing to CSV
(defn maps->csv-data
  "Converts a list of maps to be saved into a CSV file, into a list
   with a header. Assumes all maps have the same keys."
  [header-keys maps]
  (concat [(map csk/->snake_case_string header-keys)]
          (map (apply juxt header-keys) maps)))

(defn state->csv-rows
  [state]
  (->> state
       state->rows
       (maps->csv-data [:short-name :is-selected :order-index])))

;; Test pure state->csv-rows logic
(comment
  (let [state {:selected-cards  ["Q of SP"]
               :remaining-cards ["K of CL"]}
        csv-rows (state->csv-rows state)]
    (assert (first csv-rows) ["short_name" "is_selected" "order_index"])
    (assert (= (set (rest csv-rows))
               #{["Q of SP" true 0]
                 ["K of CL" false 0]}))))

;; Write final-state to a csv
(defn write-card-rows-to-csv! [cards]
  (with-open [writer (io/writer "cards.csv")]
    (csv/write-csv writer
                   cards)))

(defn read-csv-rows
  "Reads the cards csv contents in raw form."
  []
  (with-open [reader (io/reader "cards.csv")]
    (doall
     (csv/read-csv reader))))

(defn csv-rows->maps
  "Converts the raw csv rows into tidy Clojure maps.
   It does not do any coercion, but it does transform keys to kebab case."
  [csv-data]
  (map zipmap
       (->> (first csv-data)
            (map csk/->kebab-case-keyword)
            repeat)
       (rest csv-data)))

(defn rows->state
  [card-rows]
  {:selected-cards (->> card-rows
                        (filter :is-selected)
                        (sort-by :order-index)
                        (map :short-name))
   :remaining-cards (->> card-rows
                        (remove :is-selected)
                        (sort-by :order-index)
                        (map :short-name))})

(defn read-state-from-csv
  []
  (->> (read-csv-rows)
       csv-rows->maps
       ;; Perform coercion on strings for columns with non-string types
       (map #(update % :is-selected read-string))
       (map #(update % :order-index read-string))
       rows->state))

(defn read-state-from-db
  []
  (->> (read-db-rows)
       rows->state))

(defn -main
  "Runs all challenge steps, printing the results to stdout."
  []
  ;; Initialise the state before we run the program.
  ;; * Create a deck of cards.
  (println "Creating a deck of cards")
  (def stateful-card-deck (atom card-deck))
  (println "Card deck:")
  (pp/pprint @stateful-card-deck)

  ;; * Shuffle the cards.
  (println "Shuffling the cards")
  (swap! stateful-card-deck shuffle)
  (println "Card deck:")
  (pp/pprint @stateful-card-deck)

  ;; * Pick 10 cards at random.
  (println "Picking 10 cards at random")
  (def state (atom (select-cards (initial-state @stateful-card-deck))))

  ;; * Show selected and remaining cards in the deck.
  (print-deck-state @state)

  ;; * Repeat a couple of times
  (println "Repeating first time")
  (swap! state select-cards)
  (print-deck-state @state)
  (println "Repeating second time")
  (swap! state select-cards)
  (print-deck-state @state)

  ;; * Dump the results to a database of your choice (in-memory welcome)
  ;; Persist state to the db
  (println "Writing to db")
  (start-db!)
  (-> @state
      state->db-rows
      write-card-rows-to-db!)

  ;; Persist state to csv
  (println "Writing to csv")
  (-> @state
      state->csv-rows
      write-card-rows-to-csv!)

  ;; Check the persisted state in the db and csv is consistent with in-process state
  (println "Performing checks on DB")
  (assert (= (read-state-from-db)
             @state))

  (println "Performing checks on CSV")
  (assert (= (read-state-from-csv)
             @state))

  ;; Finally, print the results from the DB, and CSV
  (println "Printing results from DB")
  (print-deck-state (read-state-from-db))

  (println "Printing results from CSV")
  (print-deck-state (read-state-from-csv)))

(comment
 (-main))