## Basic example

In this example, the pyspark-shell is used to show a simple usage of the `source{d} Engine`.

First, you can see how to import the package and instantiate and object that provide all the methods to manipulate the data retrieved from repositories.

The `engine` object is used to get all the repositories, get the `HEAD` references from the repositories and eventually, get all the blobs from these references. Then a table is showed selecting the columns `blob_id`, `path` and `content`.

Launch pyspark-shell, replacing `[version]` with the [latest engine version](http://search.maven.org/#search%7Cga%7C1%7Ctech.sourced):
```sh
$ pyspark --packages "tech.sourced:engine:[version]"
```

Code
```python
from sourced.engine import Engine
engine = Engine(spark, '/path/to/siva-files')
engine.repositories.references.head_ref.commits.tree_entries.blobs.select('blob_id', 'path', 'content').show()

''' Output:
+--------------------+--------------------+--------------------+
|           blob_id  |                path|             content|
+--------------------+--------------------+--------------------+
|ff4fa0794274a7ffb...|fibonacci/fibonac...|[64 65 66 20 66 6...|
|7268016814b8ab7bc...|          gcd/gcd.py|[69 6D 70 6F 72 7...|
|25dbfff34dcc8d252...|           README.md|[23 20 66 75 6E 6...|
|b2675a52ed6bfdfa9...|prime/is_prime_op...|[69 6D 70 6F 72 7...|
|63bd495dce1d53092...|factorial/factori...|[69 6D 70 6F 72 7...|
|bf17d9730e43f5697...|         .travis.yml|[6C 61 6E 67 75 6...|
|a697a655a7bfd6ba1...|   prime/is_prime.py|[64 65 66 20 69 7...|
|76052f368f4c9c8de...|pythagorean_tripl...|[66 72 6F 6D 20 7...|
|3be2253ba2e871d3b...|prime/is_prime_op...|[69 6D 70 6F 72 7...|
|1ec7f95f8be7bf4f3...|prime/is_prime_op...|[69 6D 70 6F 72 7...|
|7268016814b8ab7bc...|          gcd/gcd.py|[69 6D 70 6F 72 7...|
|793b6e21f2eebe900...|gcd/gcd_optimal_e...|[69 6D 70 6F 72 7...|
|4d3617f27e277e4b5...|differentiation/s...|[66 72 6F 6D 20 7...|
|4d3617f27e277e4b5...|differentiation/s...|[66 72 6F 6D 20 7...|
|6d7c6cb29abb52fc2...|          gcd/gcd.py|[64 65 66 20 67 6...|
|8ab978a56c5dcb239...|factorial/factori...|[64 65 66 20 66 6...|
|e35a52f431feac4b7...|          abs/abs.py|[69 6D 70 6F 72 7...|
|b2675a52ed6bfdfa9...|prime/is_prime_op...|[69 6D 70 6F 72 7...|
|51bdeff4494d60bb7...|euclidean/distanc...|[69 6D 70 6F 72 7...|
|6d7c6cb29abb52fc2...|          gcd/gcd.py|[64 65 66 20 67 6...|
+--------------------+--------------------+--------------------+
only showing top 20 rows
'''
```
