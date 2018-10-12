(ns db-lib.core
  (:require [clojure.set :as cset]
            [clojure.string :as cstring]
            [clojure.java.io :as io])
  (:import [java.io ByteArrayOutputStream
                    ObjectOutputStream
                    ByteArrayInputStream
                    ObjectInputStream]))

(def collections
     (atom {}))

(def file-path
     (atom "db/"))

(def file-extension
     ".db")

(def collections-names
     "collections")

(def collections-sufix
     "_collection")

(def unique-indexes-sufix
     "_indexes_unique")

(def ttl-indexes-sufix
     "_indexes_ttl")

(def list-indexes-fn-ref
     (atom nil))

(def exists-by-filter-fn-ref
     (atom nil))

(def ttl-deletion-fn-ref
     (atom nil))

(def ttl-deletion-thread
     (atom nil))

(defn write-object
  "Serializes an object to disk so it can be opened again later.
   Careful: It will overwrite an existing file at file-path."
  [collection-name
   clj-obj]
  (let [file-name (str
                    @file-path
                    collection-name
                    file-extension)]
    (with-open [wr (io/writer
                     file-name)]
      (try
        (.write
          wr
          (pr-str
            clj-obj))
        (catch Exception e
          (println e))
       )
     ))
 )

(defn read-object
  "Deserialize an object from disk"
  [collection-name]
  (let [file-name (str
                    @file-path
                    collection-name
                    file-extension)
        file-name (io/file
                    file-name)]
    (when (.exists
            file-name)
      (try
        (let [file-content (slurp
                             file-name)
              read-obj (read-string
                         file-content)
              read-obj (apply
                         conj
                         (sorted-set-by
                           (fn [{idval1 :_id}
                                {idval2 :_id}]
                             (< idval1
                                idval2))
                          )
                         read-obj)]
          (swap!
            collections
            assoc
            collection-name
            read-obj))
        (catch Exception e
          (println e))
       ))
   ))

(defn get-collection
  "Get collection from database"
  [collection-name]
  (let [collection (atom nil)]
    (when (or (empty?
                @collections)
              (not
                (contains?
                  @collections
                  collection-name))
           )
      (try
        (read-object
          collection-name)
        (catch Exception e
          (println e)
          [])
       ))
    (if (contains?
          @collections
          collection-name)
      (reset!
        collection
        (get
          @collections
          collection-name))
      (reset!
        collection
        (sorted-set-by
          (fn [{idval1 :_id}
               {idval2 :_id}]
            (< idval1
               idval2))
         ))
     )
    @collection))

(defn constraints-recur-iii
  "Iterate through element fields for checking the uniqueness of field"
  [element
   fields
   new-obj
   i]
  (if-not (< i
             (count
               fields))
    true
    (let [field (get
                  fields
                  i)
          e-val (get
                  element
                  field)
          n-val (get
                  new-obj
                  field)]
      (if (= e-val
             n-val)
        false
        (recur
          element
          fields
          new-obj
          (inc
            i))
       ))
   ))

(defn constraints-recur-ii
  "Iterate through collection for checking all its elements"
  [collection-vec
   fields
   new-obj
   i]
  (if-not (< i
             (count
               collection-vec))
    true
    (let [element (get
                    collection-vec
                    i)]
      (if-not (constraints-recur-iii
                element
                fields
                new-obj
                0)
        false
        (recur
          collection-vec
          fields
          new-obj
          (inc
            i))
       ))
   ))

(defn constraints-recur
  "Check one index at a time
   if unique check its existance
   if not unique proceed to next index"
  [collection-vec
   collection-indexes
   new-obj
   i]
  (if-not (< i
             (count
               collection-indexes))
    true
    (let [{fields :fields
           unique :unique} (get
                             collection-indexes
                             i)]
      (if unique
        (constraints-recur-ii
          collection-vec
          fields
          new-obj
          0)
        (recur
          collection-vec
          collection-indexes
          new-obj
          (inc
            i))
       ))
   ))

(defn constraints?
  "Check for unique indexes"
  [collection-set
   collection-indexes
   new-obj]
  (let [collection-vec (into
                         []
                         collection-set)]
    (constraints-recur
      collection-vec
      collection-indexes
      new-obj
      0))
 )

(defn insert-one
  "Insert one object in collection"
  [collection-name
   new-obj
   & [collection-info]]
  (let [collection-set (get-collection
                         collection-name)
        collection-indexes (:unique
                             (@list-indexes-fn-ref
                               collection-name))
        constraints (constraints?
                      collection-set
                      collection-indexes
                      new-obj)]
    (when constraints
      (let [last-id (last
                      collection-set)
            new-id (if last-id
                     (:_id last-id)
                     0)
            new-obj (assoc
                      new-obj
                      :_id
                      (inc
                        new-id))
            collection-set (conj
                             collection-set
                             new-obj)]
        (swap!
          collections
          assoc
          collection-name
          collection-set)
        (write-object
          collection-name
          collection-set)
        (when (not
                (@exists-by-filter-fn-ref
                  collections-names
                  {:name collection-name}))
          (let [attrs-a (atom
                          {:name collection-name})]
            (if collection-info
              (swap!
                attrs-a
                conj
                collection-info)
              (swap!
                attrs-a
                assoc
                :type
                collections-sufix))
            (insert-one
              collections-names
              @attrs-a))
         ))
     ))
 )

(defn insert-many
  "Insert many objects in database"
  [collection-name
   new-objs]
  (try
    (doseq [new-obj new-objs]
      (insert-one
        collection-name
        new-obj))
    (catch Exception e
      (println e))
   ))

(defn find-by-id
  "Find object by id in collection"
  [collection-name
   id]
  (let [collection-set (get-collection
                         collection-name)]
    (get
      collection-set
      {:_id id}))
 )

(defn match-or-vector
  "Find if any element of or-vector is contained in element from collection"
  [or-vector
   element
   i]
  (if (< i
         (count
           or-vector))
    (let [{attr-key :attr-key
           attr-value :attr-value} (get
                                     or-vector
                                     i)
          el-val (get
                   element
                   attr-key)]
      (if (map?
            attr-value)
        (let [contains-val (get
                             attr-value
                             :contains)]
          (if (cstring/index-of
                el-val
                contains-val)
            true
            (recur
              or-vector
              element
              (inc
                i))
           ))
        (if (= attr-value
               el-val)
          true
          (recur
            or-vector
            element
            (inc
              i))
         ))
     )
    false))

(defn build-predicates-recur
  "Check if value in filter and single element from collection for same key
   are the same, and if key is :or do or matching procedure"
  [filter-keys
   filter-map
   element
   index]
  (if (< index
         (count
           filter-keys))
    (let [e-key (get
                  filter-keys
                  index)]
      (if (= e-key
             :or)
        (let [or-vector (get
                          filter-map
                          e-key)]
          (match-or-vector
            or-vector
            element
            0))
        (let [filter-val (get
                           filter-map
                           e-key)
              element-val (get
                            element
                            e-key)]
          (if-not (= filter-val
                     element-val)
            false
            (recur
              filter-keys
              filter-map
              element
              (inc
                index))
           ))
       ))
    true))

(defn build-predicates
  "Build predicate for selection function"
  [filter-map]
  (fn [element]
    (let [filter-keys (into
                        []
                        (keys
                          filter-map))]
      (build-predicates-recur
        filter-keys
        filter-map
        element
        0))
   ))

(defn select-data
  "Select data from set by custom predicate"
  [collection-set
   filter-map]
  (let [predicates? (build-predicates
                      filter-map)]
    (cset/select
      predicates?
      collection-set))
 )

(defn sort-compare-recur
  "Generates custom sort comparator"
  [el1
   el2
   sort-vec
   i]
  (if-not (< i
             (count
               sort-vec))
    true
    (let [[field
           direction] (get
                        sort-vec
                        i)
           el1-value (get
                       el1
                       field)
           el2-value (get
                       el2
                       field)]
      (if (not= (type
                  el1-value)
                (type
                  el2-value))
        true
        (if (number?
              el1-value)
          (if (not= el1-value
                    el2-value)
            (if (= direction
                   -1)
              (< el2-value
                 el1-value)
              (< el1-value
                 el2-value))
            (recur
              el1
              el2
              sort-vec
              (inc
                i))
           )
          (if (string?
                el1-value)
            (if (not= el1-value
                      el2-value)
              (if (= direction
                     -1)
                (compare
                  el2-value
                  el1-value)
                (compare
                  el1-value
                  el2-value))
              (recur
                el1
                el2
                sort-vec
                (inc
                  i))
             )
            (if-not (instance?
                      java.util.Date
                      el1-value)
              true
              (if (not= el1-value
                        el2-value)
                (if (= direction
                       -1)
                  (< (.getTime el2-value)
                     (.getTime el1-value))
                  (< (.getTime el1-value)
                     (.getTime el2-value))
                 )
                (recur
                  el1
                  el2
                  sort-vec
                  (inc
                    i))
               ))
           ))
       ))
   ))

(defn find-by-filter
  "Find elements by filter and optional parameters: projection sort limit and skip"
  [collection-name
   & [filter-map
      projection-vector
      sort-map
      limit
      skip
      collation]]
  (let [collection-set (get-collection
                         collection-name)
        filtered (if (and filter-map
                          (not
                            (empty?
                              filter-map))
                      )
                   (select-data
                     collection-set
                     filter-map)
                   collection-set)
        projected (if projection-vector
                    (cset/project
                      filtered
                      (conj
                        projection-vector
                        :_id))
                    filtered)
        sort-fn (when sort-map
                  (let [sort-vec (into
                                   []
                                   sort-map)]
                    (fn [el1 el2]
                      (sort-compare-recur
                        el1
                        el2
                        sort-vec
                        0))
                   ))
        projected (if sort-fn
                    (apply
                      sorted-set-by
                      sort-fn
                      projected)
                    (apply
                      sorted-set-by
                      (fn [{idval1 :_id}
                           {idval2 :_id}]
                        (< idval1
                           idval2))
                      projected))
        projected (into
                    []
                    projected)
        index-from (if skip
                     skip
                     0)
        index-to (if limit
                   (if (= limit
                          0)
                     (count
                       projected)
                     (let [index-to (+ index-from
                                       limit)
                           projected-count (count
                                             projected)]
                       (if (< index-to
                              projected-count)
                         index-to
                         projected-count))
                    )
                   (count
                     projected))
        index-range (range
                      index-from
                      index-to)
        result (atom [])]
    (doseq [i index-range]
      (let [element (get
                      projected
                      i)]
        (swap!
          result
          conj
          element))
     )
    @result))

(defn find-one-by-filter
  "Find element by filter and optional projection parameter"
  [collection-name
   & [filter-map
      projection-map]]
  (first
    (find-by-filter
      collection-name
      filter-map
      projection-map))
 )

(defn update-by-id
  "Update element by id and its updated values"
  [collection-name
   id
   update-obj]
  (let [collection-set (get-collection
                         collection-name)
        collection-indexes (:unique
                             (@list-indexes-fn-ref
                               collection-name))
        constraints (constraints?
                      collection-set
                      collection-indexes
                      update-obj)]
    (when constraints
      (let [old-obj (get
                      collection-set
                      {:_id id})
            old-obj-a (atom old-obj)]
        (when old-obj
          (doseq [[e-key
                   e-value] update-obj]
            (swap!
              old-obj-a
              assoc
              e-key
              e-value))
          (let [collection-set (disj
                                 collection-set
                                 {:_id id})
                collection-set (conj
                                 collection-set
                                 @old-obj-a)]
            (swap!
              collections
              assoc
              collection-name
              collection-set)
            (write-object
              collection-name
              collection-set))
         ))
     ))
 )

(defn delete-by-id
  "Delete element by id"
  [collection-name
   id]
  (let [collection-set (get-collection
                         collection-name)
        collection-set (disj
                         collection-set
                         {:_id id})]
    (swap!
      collections
      assoc
      collection-name
      collection-set)
    (write-object
      collection-name
      collection-set))
 )

(defn delete-by-filter
  "Delete elements by filter"
  [collection-name
   filter-map]
  (let [filtered (find-by-filter
                   collection-name
                   filter-map)]
    (doseq [{idval :_id} filtered]
      (delete-by-id
        collection-name
        idval))
   ))

(defn count-by-filter
  "Count elements by filter"
  [collection-name
   filter-map]
  (let [filtered (find-by-filter
                   collection-name
                   filter-map)]
    (count
      filtered))
 )

(defn exists-by-filter
  "Check existence of element by filter"
  [collection-name
   filter-map]
  (< 0
     (count-by-filter
       collection-name
       filter-map))
 )

(reset!
  exists-by-filter-fn-ref
  exists-by-filter)

(defn create-unique-index
  "Create unique index on collection and particular element fields"
  [collection-name
   fields
   index-name]
  (let [unique-exists (exists-by-filter
                        (str
                          collection-name
                          unique-indexes-sufix)
                        {:name index-name})
        ttl-exists (exists-by-filter
                     (str
                       collection-name
                       ttl-indexes-sufix)
                     {:name index-name})]
    (when (and (not unique-exists)
               (not ttl-exists))
      (when (and (string? index-name)
                 (vector? fields))
        (let [idx-collection-name (str
                                    collection-name
                                    unique-indexes-sufix)]
          (insert-one
            idx-collection-name
            {:name index-name
             :fields fields
             :unique true}
            {:collection collection-name
             :type unique-indexes-sufix}))
       ))
   ))

(defn create-ttl-index
  "Create ttl (Time-To-Live) index on collection and particular element field (Date field)
   with a parameter expire-after-seconds that represents element deletion after
   number of seconds from date field"
  [collection-name
   field
   index-name
   expire-after-seconds]
  (let [unique-exists (exists-by-filter
                        (str
                          collection-name
                          unique-indexes-sufix)
                        {:name index-name})
        ttl-exists (exists-by-filter
                     (str
                       collection-name
                       ttl-indexes-sufix)
                     {:name index-name})]
    (when (and (not unique-exists)
               (not ttl-exists))
      (when (and (string? index-name)
                 (keyword? field)
                 (instance?
                   java.lang.Long
                   expire-after-seconds))
        (let [idx-collection-name (str
                                    collection-name
                                    ttl-indexes-sufix)]
          (insert-one
            idx-collection-name
            {:name index-name
             :field field
             :expire-after-seconds expire-after-seconds}
            {:collection collection-name
             :type ttl-indexes-sufix}))
        (when (or (nil? @ttl-deletion-thread)
                  (future-done?
                    @ttl-deletion-thread))
          (reset!
            ttl-deletion-thread
            (future
              (@ttl-deletion-fn-ref))
           ))
       ))
   ))

(defn list-indexes
  "List all indexes for collection"
  [collection-name]
  (let [result (atom {})]
    (swap!
      result
      assoc
      :unique
      (find-by-filter
        (str
          collection-name
          unique-indexes-sufix))
     )
    (swap!
      result
      assoc
      :ttl
      (find-by-filter
        (str
          collection-name
          ttl-indexes-sufix))
     )
    @result))

(reset!
  list-indexes-fn-ref
  list-indexes)

(defn find-index
  "Find index on collection by index name"
  [collection-name
   index-name]
  (let [result-unique (find-one-by-filter
                        (str
                          collection-name
                          unique-indexes-sufix)
                        {:name index-name})
        result-ttl (find-one-by-filter
                     (str
                       collection-name
                       ttl-indexes-sufix)
                     {:name index-name})
        result (atom nil)]
    (when result-unique
      (reset!
        result
        result-unique))
    (when result-ttl
      (reset!
        result
        result-ttl))
    @result))

(defn index-exists?
  "Check if index exists on collection by index name"
  [collection-name
   index-name]
  (let [result-unique (exists-by-filter
                        (str
                          collection-name
                          unique-indexes-sufix)
                        {:name index-name})
        result-ttl (exists-by-filter
                     (str
                       collection-name
                       ttl-indexes-sufix)
                     {:name index-name})
        result (atom false)]
    (when result-unique
      (reset!
        result
        true))
    (when result-ttl
      (reset!
        result
        true))
    @result))

(defn drop-index
  "Drop index from collection by index name"
  [collection-name
   index-name]
  (delete-by-filter
    (str
      collection-name
      unique-indexes-sufix)
    {:name index-name})
  (delete-by-filter
    (str
      collection-name
      ttl-indexes-sufix)
    {:name index-name}))

(defn is-expired?
  "Check if ttl (Time-To-Live) index is expired"
  [date-value
   expire-after-seconds]
  (let [current-date-time (java.util.Date.)
        current-time (java.util.Calendar/getInstance)
        expiration-time (java.util.Calendar/getInstance)]
    (.setTime
      current-time
      current-date-time)
    (.setTime
      expiration-time
      date-value)
    (.add
      expiration-time
      java.util.Calendar/SECOND
      expire-after-seconds)
    (.after
      current-time
      expiration-time))
 )

(defn ttl-deletion
  "Time-To-Live function that executes every 1 minute for ttl index expiration check"
  []
  (let [ttl-collections (atom
                          (find-by-filter
                            collections-names
                            {:type ttl-indexes-sufix}))]
    (while (not
             (empty?
               @ttl-collections))
      (doseq [{ttl-name :name
               ttl-collection :collection} @ttl-collections]
        (let [indexes-list (find-by-filter
                             ttl-name)
              ids-for-deletion (atom #{})]
          (doseq [{field :field
                   expire-after-seconds :expire-after-seconds} indexes-list]
            (let [collection-set (find-by-filter
                                   ttl-collection)]
              (doseq [{field-value field
                       id :_id} collection-set]
                (when (and (instance?
                             java.util.Date
                             field-value)
                            (is-expired?
                              field-value
                              expire-after-seconds))
                  (swap!
                    ids-for-deletion
                    conj
                    id))
               ))
           )
          (doseq [id-for-deletion @ids-for-deletion]
            (delete-by-id
              ttl-collection
              id-for-deletion))
         ))
      (Thread/sleep (* 60 1000))
      (reset!
        ttl-collections
        (find-by-filter
          collections-names
          {:type ttl-indexes-sufix}))
     ))
 )

(reset!
  ttl-deletion-fn-ref
  ttl-deletion)

(defn connect
  ""
  [& [file-path-p]]
  (when file-path-p
    (reset!
      file-path
      file-path-p))
  (reset!
    ttl-deletion-thread
    (future
      (ttl-deletion))
   ))

