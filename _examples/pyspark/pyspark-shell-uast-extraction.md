## Extracting UASTs example

In the example code below, you can take a look to how the `extract_uasts()` method works.

From the `engine` object instantiated in the spark-shell, a bunch of blobs are retrieving from the `HEAD` references from all the repositories and requesting for them. Once we have those blobs, we can call `extract_uasts()` which send the blobs to a [bblfsh server](https://github.com/bblfsh/server) to get back the UASTs.

Finally, the `blob_id` , `path` and `uast` is showed on the table.

Launch pyspark-shell, replacing `[version]` with the [latest engine version](http://search.maven.org/#search%7Cga%7C1%7Ctech.sourced):
```sh
$ pyspark --packages "tech.sourced:engine:[version]"
```

Code:
```python
from sourced.engine import Engine
engine = Engine(spark, '/path/to/siva-files')
engine.repositories.references.head_ref.commits.tree_entries.blobs.classify_languages().extract_uasts().select("blob_id", "path", "uast").show()

''' Output:
+--------------------+--------------------+-------------+
|             blob_id|                path|         uast|
+--------------------+--------------------+-------------+
|ff4fa0794274a7ffb...|fibonacci/fibonac...|[[B@43efe672]|
|7268016814b8ab7bc...|          gcd/gcd.py|[[B@66938491]|
|25dbfff34dcc8d252...|           README.md|           []|
|b2675a52ed6bfdfa9...|prime/is_prime_op...|[[B@51261a61]|
|63bd495dce1d53092...|factorial/factori...|[[B@3163c734]|
|bf17d9730e43f5697...|         .travis.yml|           []|
|a697a655a7bfd6ba1...|   prime/is_prime.py| [[B@d036b1c]|
|76052f368f4c9c8de...|pythagorean_tripl...|[[B@774ec121]|
|3be2253ba2e871d3b...|prime/is_prime_op...|[[B@16da28bb]|
|1ec7f95f8be7bf4f3...|prime/is_prime_op...|[[B@39af1733]|
|7268016814b8ab7bc...|          gcd/gcd.py|[[B@2f62c091]|
|793b6e21f2eebe900...|gcd/gcd_optimal_e...|[[B@2e245b95]|
|4d3617f27e277e4b5...|differentiation/s...|[[B@697c211a]|
|4d3617f27e277e4b5...|differentiation/s...|[[B@282bb589]|
|6d7c6cb29abb52fc2...|          gcd/gcd.py|[[B@11f49e55]|
|8ab978a56c5dcb239...|factorial/factori...|[[B@1d80870d]|
|e35a52f431feac4b7...|          abs/abs.py|[[B@157c0156]|
|b2675a52ed6bfdfa9...|prime/is_prime_op...|[[B@608e698d]|
|51bdeff4494d60bb7...|euclidean/distanc...|[[B@55bd45ff]|
|6d7c6cb29abb52fc2...|          gcd/gcd.py|[[B@4c1c08aa]|
+--------------------+--------------------+-------------+
only showing top 20 rows
'''
```
