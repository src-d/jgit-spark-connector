## Classifying languages and extracting UASTs example

The combined usage of both, `classify_languages()` and `extract_uasts()` methods, has the advantage that doesn't rely the language detection task on the [bblfsh server](https://github.com/bblfsh/server) , so you can save some time.

To do that, you just have to call  `extract_uasts()` on a Dataframe where previously, `classify_languages()` was used.

Launch pyspark-shell, replacing `[version]` with the [latest engine version](http://search.maven.org/#search%7Cga%7C1%7Ctech.sourced):
```sh
$ pyspark --packages "tech.sourced:engine:[version]"
```

Code:
```python
from sourced.engine import Engine
engine = Engine(spark, '/path/to/siva-files')
engine.repositories.references.head_ref.commits.tree_entries.blobs.classify_languages().extract_uasts().select("path", "lang", "uast").show()

''' Output:
+--------------------+--------+-------------+
|                path|    lang|         uast|
+--------------------+--------+-------------+
|fibonacci/fibonac...|  Python|[[B@759dfd4e]|
|          gcd/gcd.py|  Python| [[B@36ea40c]|
|           README.md|Markdown|           []|
|prime/is_prime_op...|  Python|[[B@2da632d5]|
|factorial/factori...|  Python|  [[B@37e738]|
|         .travis.yml|    YAML|           []|
|   prime/is_prime.py|  Python|[[B@1ada1dfd]|
|pythagorean_tripl...|  Python|[[B@6ce2846e]|
|prime/is_prime_op...|  Python|[[B@704e33bd]|
|prime/is_prime_op...|  Python|[[B@4fff14ab]|
|          gcd/gcd.py|  Python| [[B@580cd5c]|
|gcd/gcd_optimal_e...|  Python|[[B@7db9e876]|
|differentiation/s...|  Python|[[B@7c6befa7]|
|differentiation/s...|  Python|[[B@4b06f6cd]|
|          gcd/gcd.py|  Python|[[B@486f38dc]|
|factorial/factori...|  Python|[[B@7a2783ff]|
|          abs/abs.py|  Python|[[B@59124dcb]|
|prime/is_prime_op...|  Python|[[B@25de68ba]|
|euclidean/distanc...|  Python|[[B@14c61d05]|
|          gcd/gcd.py|  Python|[[B@52b84c19]|
+--------------------+--------+-------------+
only showing top 20 rows
'''
```
