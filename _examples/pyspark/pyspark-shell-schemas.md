```bash
$ pyspark --packages com.github.src-d:spark-api:master-SNAPSHOT --repositories https://jitpack.io
>>> from sourced.spark import API as SparkAPI
>>> api = SparkAPI(spark, '/path/to/siva-files')

>>> api.repositories.printSchema()
root
 |-- id: string (nullable = true)
 |-- urls: array (nullable = true)
 |    |-- element: string (containsNull = false)
 |-- is_fork: boolean (nullable = true)

>>> api.repositories.references.printSchema()
root
 |-- repository_id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- hash: string (nullable = true)

>>> api.repositories.references.files.printSchema()
root
 |-- file_hash: string (nullable = true)
 |-- content: binary (nullable = true)
 |-- commit_hash: string (nullable = true)
 |-- is_binary: boolean (nullable = false)
 |-- path: string (nullable = true)
 |-- repository_id: string (nullable = true)
 |-- name: string (nullable = true)

>>> api.repositories.references.commits.printSchema()
root
 |-- repository_id: string (nullable = true)
 |-- reference_name: string (nullable = true)
 |-- index: integer (nullable = true)
 |-- hash: string (nullable = true)
 |-- message: string (nullable = true)
 |-- parents: array (nullable = true)
 |    |-- element: string (containsNull = false)
 |-- tree: map (nullable = true)
 |    |-- key: string
 |    |-- value: string (valueContainsNull = false)
 |-- blobs: array (nullable = true)
 |    |-- element: string (containsNull = false)
 |-- parents_count: integer (nullable = true)
 |-- author_email: string (nullable = true)
 |-- author_name: string (nullable = true)
 |-- author_date: timestamp (nullable = true)
 |-- committer_email: string (nullable = true)
 |-- committer_name: string (nullable = true)
 |-- committer_date: timestamp (nullable = true)

```
