## Printing schema example

The next example showed,  just try to show the simple usage of the useful method `printSchema()`.

It can help you to follow the aggregated or pruned information that your transformations make on the data you are handling.

```python
$ pyspark --packages com.github.src-d:engine:master-SNAPSHOT --repositories https://jitpack.io
>>> from sourced.engine import Engine
>>> engine = Engine(spark, '/path/to/siva-files')

>>> engine.repositories.printSchema()
root
 |-- id: string (nullable = false)
 |-- urls: array (nullable = false)
 |    |-- element: string (containsNull = false)
 |-- is_fork: boolean (nullable = true)

>>> engine.repositories.references.printSchema()
root
 |-- repository_id: string (nullable = false)
 |-- name: string (nullable = false)
 |-- hash: string (nullable = false)

>>> engine.repositories.references.commits.printSchema()
root
 |-- repository_id: string (nullable = false)
 |-- reference_name: string (nullable = false)
 |-- index: integer (nullable = false)
 |-- hash: string (nullable = false)
 |-- message: string (nullable = false)
 |-- parents: array (nullable = true)
 |    |-- element: string (containsNull = false)
 |-- tree: map (nullable = true)
 |    |-- key: string
 |    |-- value: string (valueContainsNull = false)
 |-- blobs: array (nullable = true)
 |    |-- element: string (containsNull = false)
 |-- parents_count: integer (nullable = false)
 |-- author_email: string (nullable = true)
 |-- author_name: string (nullable = true)
 |-- author_date: timestamp (nullable = true)
 |-- committer_email: string (nullable = true)
 |-- committer_name: string (nullable = true)
 |-- committer_date: timestamp (nullable = true)

>>> engine.repositories.references.files.printSchema()
root
 |-- file_hash: string (nullable = false)
 |-- content: binary (nullable = true)
 |-- commit_hash: string (nullable = false)
 |-- is_binary: boolean (nullable = false)
 |-- path: string (nullable = true)

>>> engine.repositories.references.files.classify_languages().printSchema()
root
 |-- file_hash: string (nullable = false)
 |-- content: binary (nullable = true)
 |-- commit_hash: string (nullable = false)
 |-- is_binary: boolean (nullable = false)
 |-- path: string (nullable = true)
 |-- lang: string (nullable = true)

>>> engine.repositories.references.files.classify_languages().extract_uasts().printSchema()
root
 |-- file_hash: string (nullable = false)
 |-- content: binary (nullable = true)
 |-- commit_hash: string (nullable = false)
 |-- is_binary: boolean (nullable = false)
 |-- path: string (nullable = true)
 |-- lang: string (nullable = true)
 |-- uast: array (nullable = true)
 |    |-- element: binary (containsNull = true)

```
