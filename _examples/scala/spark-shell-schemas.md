## Printing schema example

The next example showed,  just try to show the simple usage of the useful method `printSchema`.

It can help you to follow the aggregated or pruned information that your transformations make on the data you are handling.

Launch spark-shell:
```sh
$ spark-shell --packages "tech.sourced:engine:0.1.2"
```

Code:
```scala
import tech.sourced.engine._

val engine = Engine(spark, "/path/to/siva-files")
engine.getRepositories.printSchema
/* Output:
root
 |-- id: string (nullable = false)
 |-- urls: array (nullable = false)
 |    |-- element: string (containsNull = false)
 |-- is_fork: boolean (nullable = true)
*/

engine.getRepositories.getReferences.printSchema
/* Output:
root
 |-- repository_id: string (nullable = false)
 |-- name: string (nullable = false)
 |-- hash: string (nullable = false)
*/

engine.getRepositories.getReferences.getCommits.printSchema
/* Output:
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
*/

engine.getRepositories.getReferences.getFiles.printSchema
/* Output:
root
 |-- file_hash: string (nullable = false)
 |-- content: binary (nullable = true)
 |-- commit_hash: string (nullable = false)
 |-- is_binary: boolean (nullable = false)
 |-- path: string (nullable = true)
*/

engine.getRepositories.getReferences.getFiles.classifyLanguages.printSchema
/* Output:
root
 |-- file_hash: string (nullable = false)
 |-- content: binary (nullable = true)
 |-- commit_hash: string (nullable = false)
 |-- is_binary: boolean (nullable = false)
 |-- path: string (nullable = true)
 |-- lang: string (nullable = true)
*/

engine.getRepositories.getReferences.getFiles.classifyLanguages.extractUASTs.printSchema
/* Output:
root
 |-- file_hash: string (nullable = false)
 |-- content: binary (nullable = true)
 |-- commit_hash: string (nullable = false)
 |-- is_binary: boolean (nullable = false)
 |-- path: string (nullable = true)
 |-- lang: string (nullable = true)
 |-- uast: array (nullable = true)
 |    |-- element: binary (containsNull = true)
*/
```
