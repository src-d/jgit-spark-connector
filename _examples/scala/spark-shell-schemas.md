## Printing schema example

The next example showed,  just try to show the simple usage of the useful method `printSchema`.

It can help you to follow the aggregated or pruned information that your transformations make on the data you are handling.

```bash
$ spark-shell --packages com.github.src-d:enginei:master-SNAPSHOT --repositories https://jitpack.io
scala> import tech.sourced.engine._
import tech.sourced.engine._

scala> val engine = Engine(spark, "/path/to/siva-files")

scala> engine.getRepositories.printSchema
root
 |-- id: string (nullable = true)
 |-- urls: array (nullable = true)
 |    |-- element: string (containsNull = false)
 |-- is_fork: boolean (nullable = true)


scala> engine.getRepositories.getReference.printSchema
getReference   getReferences

scala> engine.getRepositories.getReferences.printSchema
root
 |-- repository_id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- hash: string (nullable = true)


scala> engine.getRepositories.getReferences.getCommits.printSchema
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


scala> engine.getRepositories.getReferences.getFiles.printSchema
root
 |-- file_hash: string (nullable = true)
 |-- content: binary (nullable = true)
 |-- commit_hash: string (nullable = true)
 |-- is_binary: boolean (nullable = false)
 |-- path: string (nullable = true)
 |-- repository_id: string (nullable = true)
 |-- name: string (nullable = true)

```
