## Printing schema example

The next example showed,  just try to show the simple usage of the useful method `printSchema()`.

It can help you to follow the aggregated or pruned information that your transformations make on the data you are handling.

Launch pyspark-shell, replacing `[version]` with the [latest engine version](http://search.maven.org/#search%7Cga%7C1%7Ctech.sourced):
```sh
$ pyspark --packages "tech.sourced:engine:[version]"
```

Code:
```python
from sourced.engine import Engine
engine = Engine(spark, '/path/to/siva-files')

engine.repositories.printSchema()
''' Output:
root
 |-- id: string (nullable = false)
 |-- urls: array (nullable = false)
 |    |-- element: string (containsNull = false)
 |-- is_fork: boolean (nullable = true)
'''

engine.repositories.references.printSchema()
''' Output:
root
 |-- repository_id: string (nullable = false)
 |-- name: string (nullable = false)
 |-- hash: string (nullable = false)
'''

engine.repositories.references.commits.printSchema()
''' Output:
root
 |-- repository_id: string (nullable = false)
 |-- reference_name: string (nullable = false)
 |-- index: integer (nullable = false)
 |-- hash: string (nullable = false)
 |-- message: string (nullable = false)
 |-- parents: array (nullable = true)
 |    |-- element: string (containsNull = false)
 |-- parents_count: integer (nullable = false)
 |-- author_email: string (nullable = true)
 |-- author_name: string (nullable = true)
 |-- author_date: timestamp (nullable = true)
 |-- committer_email: string (nullable = true)
 |-- committer_name: string (nullable = true)
 |-- committer_date: timestamp (nullable = true)
'''

engine.repositories.references.commits.tree_entries.printSchema()
''' Output:
root
 |-- commit_hash: string (nullable = false)
 |-- repository_id: string (nullable = false)
 |-- reference_name: string (nullable = false)
 |-- path: string (nullable = true)
 |-- blob: string (nullable = false)
'''

engine.repositories.references.commits.tree_entries.blobs.printSchema()
''' Output:
root
 |-- blob_id: string (nullable = false)
 |-- commit_hash: string (nullable = false)
 |-- repository_id: string (nullable = false)
 |-- reference_name: string (nullable = false)
 |-- content: binary (nullable = true)
 |-- is_binary: boolean (nullable = false)
 |-- path: string (nullable = true)
'''

engine.repositories.references.commits.tree_entries.blobs.classify_languages().printSchema()
''' Output:
root
 |-- blob_id: string (nullable = false)
 |-- commit_hash: string (nullable = false)
 |-- repository_id: string (nullable = false)
 |-- reference_name: string (nullable = false)
 |-- content: binary (nullable = true)
 |-- is_binary: boolean (nullable = false)
 |-- path: string (nullable = true)
 |-- lang: string (nullable = true)
'''

engine.repositories.references.commits.tree_entries.blobs.classify_languages().extract_uasts().printSchema()
''' Output:
root
 |-- blob_id: string (nullable = false)
 |-- commit_hash: string (nullable = false)
 |-- repository_id: string (nullable = false)
 |-- reference_name: string (nullable = false)
 |-- content: binary (nullable = true)
 |-- is_binary: boolean (nullable = false)
 |-- path: string (nullable = true)
 |-- lang: string (nullable = true)
 |-- uast: array (nullable = true)
 |    |-- element: binary (containsNull = true)
'''
```
