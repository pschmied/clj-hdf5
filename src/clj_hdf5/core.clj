(ns clj-hdf5.core
  (:refer-clojure :exclude [read])
  (:require clojure.string)
  (:import java.io.File))

; Record definitions

; A node is defined by its reader/writerand its path inside that file.
(defrecord hdf-node
  [accessor path])

; An attribute is defined by its node and its name.
(defrecord hdf-attribute
  [accessor path attrname])

; Private utility definitions 
(defn- absolute-path?
  [path]
  (= (first path) \/))

(defn- path-concat
  [abs-path rel-path]
  (assert (not (absolute-path? rel-path)))
  (if (= abs-path "/")
    (str "/" rel-path)
    (str abs-path "/" rel-path)))

(def ^{:private true} byte-array-class
     (class (make-array Byte/TYPE 0)))
(def ^{:private true} short-array-class
     (class (make-array Short/TYPE 0)))
(def ^{:private true} int-array-class
     (class (make-array Integer/TYPE 0)))
(def ^{:private true} long-array-class
     (class (make-array Long/TYPE 0)))
(def ^{:private true} float-array-class
     (class (make-array Float/TYPE 0)))
(def ^{:private true} double-array-class
     (class (make-array Double/TYPE 0)))
(def ^{:private true} string-array-class
     (class (make-array java.lang.String 0)))

(def ^{:private true} integer-array-class
     (class (make-array Integer 0)))

; Type checks

(defn node?
  [object]
  (isa? (class object) hdf-node))

(defn group?
  [object]
  (and (node? object)
       (. (:accessor object)  isGroup (:path object))))

(defn dataset?
  [object]
  (and (node? object)
       (. (:accessor object)  isDataSet (:path object))))

(defn root?
  [object]
  (and (group? object)
       (= (:path object) "/")))

(defn attribute?
  [object]
  (isa? (class object) hdf-attribute))

; Opening and closing files.
; The return value of open/create is the root group object.

(defn open
  ([file] (open file :read-only))
  ([file mode]
     (assert (isa? (class file) java.io.File))
     (let [factory (ch.systemsx.cisd.hdf5.HDF5FactoryProvider/get)]
       (new hdf-node
            (case mode
                  :read-only   (. factory openForReading file)
                  :read-write  (. factory open file)
                  :create      (let [configurator (. factory configure file)]
                                 (. configurator overwrite)
                                 (. configurator writer)))
            "/"))))

(defn create
  [file]
  (open file :create))

(defn close
  [root-group]
  (assert (root? root-group))
  (. (:accessor root-group) close))

; Datatypes

(defn datatype
  [object]
  (assert (or (dataset? object)
              (attribute? object)))
  (let [acc  (:accessor object)
        path (:path object)]
    (if (dataset? object)
      (.getTypeInformation (.getDataSetInformation acc path))
      (.getAttributeInformation acc path (:attrname object)))))

; Reading datasets and attributes

(defmulti read class)

; Nodes

(defn file
  [node]
  (assert (node? node))
  (. (:accessor node) getFile))

(defn path
  [node]
  (assert (node? node))
  (:path node))

(defn parent
  [node]
  (assert (node? node))
  (let [path (clojure.string/split (:path node) #"/")]
    (if (empty? path)
      nil
      (let [parent-path (subvec path 0 (dec (count path)))]
        (new hdf-node
             (:accessor node)
             (if (= (count parent-path) 1)
               "/"
               (clojure.string/join "/" parent-path)))))))

(defn root
  [node]
  (assert (node? node))
  (new hdf-node (:accessor node) "/"))

(defn attributes
  [node]
  (assert (node? node))
  (let [acc   (:accessor node)
        path  (:path node)
        names (. acc  getAttributeNames path)]
    (into {} (for [n names]
               [n (new hdf-attribute acc path n)]))))

(defn get-attribute
  [node name]
  (assert (node? node))
  (let [acc  (:accessor node)
        path (:path node)]
    (if (. acc hasAttribute path name)
      (new hdf-attribute acc path name)
      nil)))

(defn- read-scalar-attribute
  [acc path name dclass]
  (cond
   (= dclass ch.systemsx.cisd.hdf5.HDF5DataClass/STRING)
      (. acc getStringAttribute path name)
   (= dclass ch.systemsx.cisd.hdf5.HDF5DataClass/INTEGER)
      (. acc getLongAttribute path name)
   (= dclass ch.systemsx.cisd.hdf5.HDF5DataClass/FLOAT)
      (. acc getDoubleAttribute path name)
   (= dclass ch.systemsx.cisd.hdf5.HDF5DataClass/REFERENCE)
      (new hdf-node acc  (. acc getObjectReferenceAttribute path name))
   :else
      nil))

(defn- read-array-attribute
  [acc path name dclass]
  (cond
   (= dclass ch.systemsx.cisd.hdf5.HDF5DataClass/STRING)
      (vec (. acc getStringArrayAttribute path name))
   (= dclass ch.systemsx.cisd.hdf5.HDF5DataClass/INTEGER)
      (vec (. acc getLongArrayAttribute path name))
   (= dclass ch.systemsx.cisd.hdf5.HDF5DataClass/FLOAT)
      (vec (. acc getDoubleArrayAttribute path name))
   :else
      nil))

(defmethod read hdf-attribute
  [attr]
  (let [acc    (:accessor attr)
        path   (:path attr)
        name   (:attrname attr)
        dt     (datatype attr)
        dclass (. dt getDataClass)
        ddims  (vec (. dt getDimensions))]
    (cond
     (not (.isArrayType dt))
        (read-scalar-attribute acc path name dclass)
     (> (count ddims) 1)
        (throw (Exception. "attributes with rank > 1 not implemented yet"))
     :else
         (read-array-attribute acc path name dclass))))

(defmulti create-attribute (fn [node name value] (class value)))

(defmacro ^{:private true} create-attribute-method
  [datatype method-name]
  `(defmethod ~'create-attribute ~datatype
     [~'node ~'name ~'value]
     (let [~'acc  (:accessor ~'node)
           ~'path (:path ~'node)]
       (~method-name ~'acc ~'path ~'name ~'value)
       (new ~'hdf-attribute ~'acc ~'path ~'name))))

(create-attribute-method java.lang.String .setStringAttribute)
(create-attribute-method string-array-class .setStringArrayAttribute)
(create-attribute-method java.lang.Integer .setIntAttribute)
(create-attribute-method int-array-class .setIntArrayAttribute)
(create-attribute-method integer-array-class .setIntArrayAttribute)

(defmethod create-attribute clojure.lang.Sequential
  [node name value]
  (let [el-class (get {Boolean Boolean/TYPE
                       Byte    Byte/TYPE
                       Short   Short/TYPE
                       Integer Integer/TYPE
                       Long    Long/TYPE
                       Float   Float/TYPE
                       Double  Double/TYPE} (class (first value)))]
    (create-attribute node name
                      (if el-class
                        (into-array el-class value)
                        (into-array value)))))

; Groups

(defn members
  [group]
  (assert (group? group))
  (into {}
        (for [name (. (:accessor group) getAllGroupMembers (:path group))]
          [name (new hdf-node
                     (:accessor group)
                     (path-concat (:path group) name))])))

(defn lookup
  [group name]
  (assert (group? group))
  (let [acc       (:accessor group)
        path      (:path group)
        full-path (path-concat path name)]
    (if (. acc exists full-path)
      (new hdf-node acc full-path)
      nil)))

(defn create-group
  [parent name]
  (assert (group? parent))
  (. (:accessor parent) createGroup (path-concat (:path parent) name))
  (lookup parent name))

; Datasets

(defn dimensions
  [dataset]
  (assert (dataset? dataset))
  (vec (. (. (:accessor dataset) getDataSetInformation (:path dataset))
          getDimensions)))

(defn max-dimensions
  [dataset]
  (assert (dataset? dataset))
  (vec (. (. (:accessor dataset) getDataSetInformation (:path dataset))
          getMaxDimensions)))

(defn rank
  [dataset]
  (assert (dataset? dataset))
  (. (. (:accessor dataset) getDataSetInformation (:path dataset))
     getRank))

(defmulti create-dataset
  (fn [parent name data] (type data)))

(defmulti ^{:private true} create-array-dataset
  (fn [parent name data] (type (first data))))

(defmethod create-dataset clojure.lang.Sequential
  [parent name data]
  (create-array-dataset parent name data))

(defmethod create-dataset clojure.lang.IPersistentMap
  [parent name data]
  (assert (and (= (set (keys data)) (set [:tag :data]))
               (string? (:tag data))
               (= (class (:data data)) byte-array-class)))
  (let [acc       (:accessor parent)
        path      (:path parent)
        full-path (path-concat path name)]
    (.writeOpaqueByteArray acc full-path (:tag data) (:data data))
    (new hdf-node acc full-path)))

(defmacro ^{:private true} create-dataset-method
  [datatype scalar-method-name
   array-method-name array-element-type array-type]
  `(do
     (defmethod ~'create-dataset ~datatype
       [~'parent ~'name ~'data]
       (let [~'acc       (:accessor ~'parent)
             ~'path      (:path ~'parent)
             ~'full-path (path-concat ~'path ~'name)]
         (~scalar-method-name ~'acc ~'full-path ~'data)
         (new ~'hdf-node ~'acc ~'full-path)))
     (defmethod ~'create-array-dataset ~datatype
       [~'parent ~'name ~'data]
       (let [~'acc       (:accessor ~'parent)
             ~'path      (:path ~'parent)
             ~'full-path (path-concat ~'path ~'name)]
         (~array-method-name ~'acc ~'full-path
                             (into-array ~array-element-type ~'data))
         (new ~'hdf-node ~'acc ~'full-path)))
     (defmethod ~'create-dataset ~array-type
       [~'parent ~'name ~'data]
       (let [~'acc       (:accessor ~'parent)
             ~'path      (:path ~'parent)
             ~'full-path (path-concat ~'path ~'name)]
         (~array-method-name ~'acc ~'full-path ~'data)
         (new ~'hdf-node ~'acc ~'full-path)))))

(create-dataset-method
  Byte .writeByte .writeByteArray Byte/TYPE byte-array-class)
(create-dataset-method
  Short .writeShort .writeShortArray Short/TYPE short-array-class)
(create-dataset-method
  Integer .writeInt .writeIntArray Integer/TYPE int-array-class)
(create-dataset-method
  Long .writeLong .writeLongArray Long/TYPE long-array-class)
(create-dataset-method
  Float .writeFloat .writeFloatArray Float/TYPE float-array-class)
(create-dataset-method
  Double .writeDouble .writeDoubleArray Double/TYPE double-array-class)
(create-dataset-method
  String .writeString .writeStringArray String string-array-class)

(defn- read-scalar-dataset
  [acc path dtclass]
  (cond
   (= dtclass ch.systemsx.cisd.hdf5.HDF5DataClass/STRING)
      (. acc readString path)
   (= dtclass ch.systemsx.cisd.hdf5.HDF5DataClass/INTEGER)
      (. acc readLong path)
   (= dtclass ch.systemsx.cisd.hdf5.HDF5DataClass/FLOAT)
      (. acc readDouble path)
   :else
      nil))

(defn- read-array-dataset
  [acc path dtclass]
  (cond
   (= dtclass ch.systemsx.cisd.hdf5.HDF5DataClass/STRING)
      (vec (. acc readStringArray path))
   (= dtclass ch.systemsx.cisd.hdf5.HDF5DataClass/INTEGER)
      (vec (. acc readLongArray path))
   (= dtclass ch.systemsx.cisd.hdf5.HDF5DataClass/FLOAT)
      (vec (. acc readDoubleArray path))
   (= dtclass ch.systemsx.cisd.hdf5.HDF5DataClass/OPAQUE)
      {:tag  (. acc tryGetOpaqueTag path)
       :data (. acc readAsByteArray path)}
   :else
      nil))

(defmethod read hdf-node
  [ds]
  (assert (dataset? ds))
  (let [acc     (:accessor ds)
        path    (:path ds)
        dsinfo  (.getDataSetInformation acc path)
        dims    (vec (.getDimensions dsinfo))
        dt      (datatype ds)
        dtclass (.getDataClass dt)
        dtdims  (if (.isArrayType dt)
                  (vec (.getDimensions dt))
                  [])
        rank    (+ (count dims) (count dtdims))]
    (cond
     (= rank 0)
        (read-scalar-dataset acc path dtclass)
     (= rank 1)
        (read-array-dataset acc path dtclass)
     :else
        (throw (Exception. "datasets with rank > 1 not implemented yet")))))
