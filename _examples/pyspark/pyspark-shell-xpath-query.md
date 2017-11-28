## Querying UASTs with XPath example

You can see in this example how to make queries using [XPath syntax](https://www.w3.org/TR/xpath/) to retrieve valuable information from the UASTs.

First we must use `extract_uasts()` method to request to a [bblfsh daemon](https://github.com/bblfsh/bblfshd) the UASTs.

Then we can use the method `query_uast()` to get a result for the query we are formulating requesting tokens.  This method takes in three parameters, the query, the column which contains the UASTs and the column that will be generated with the result.

Finally, `extract_tokens()` method will generate a column `tokens` based on the previous generated column `result`.

Launch pyspark-shell, replacing `[version]` with the [latest engine version](http://search.maven.org/#search%7Cga%7C1%7Ctech.sourced):
```sh
$ pyspark --packages "tech.sourced:engine:[version]"
```

Code:
```python
from sourced.engine import Engine
engine = Engine(spark, '/path/to/siva-files')

engine.repositories.references.head_ref.commits.tree_entries.blobs.classify_languages().where('lang = "Python"').extract_uasts().query_uast('//*[@roleIdentifier]').extract_tokens('result', 'tokens').select('blob_id', 'path', 'lang', 'uast', 'tokens').show()

''' Output:
+--------------------+--------------------+------+-------------+--------------------+
|             blob_id|                path|  lang|         uast|              tokens|
+--------------------+--------------------+------+-------------+--------------------+
|ff4fa0794274a7ffb...|fibonacci/fibonac...|Python|[[B@617b4738]|[fibonacci, n, in...|
|7268016814b8ab7bc...|          gcd/gcd.py|Python|[[B@2c66d0f9]|[math, gcd, a, in...|
|b2675a52ed6bfdfa9...|prime/is_prime_op...|Python|[[B@59c072af]|[math, is_prime, ...|
|63bd495dce1d53092...|factorial/factori...|Python|[[B@45b32617]|[math, factorial,...|
|a697a655a7bfd6ba1...|   prime/is_prime.py|Python|[[B@7ecafb1e]|[is_prime, n, int...|
|76052f368f4c9c8de...|pythagorean_tripl...|Python|[[B@64311d26]|[typing, List, ty...|
|3be2253ba2e871d3b...|prime/is_prime_op...|Python|[[B@3e3e5e05]|[math, random, RA...|
|1ec7f95f8be7bf4f3...|prime/is_prime_op...|Python|[[B@62e1544b]|[math, is_prime_o...|
|7268016814b8ab7bc...|          gcd/gcd.py|Python|[[B@4b5a5102]|[math, gcd, a, in...|
|793b6e21f2eebe900...|gcd/gcd_optimal_e...|Python|[[B@27eead62]|[math, gcd_optima...|
|4d3617f27e277e4b5...|differentiation/s...|Python|[[B@6b6c11ec]|[typing, Callable...|
|4d3617f27e277e4b5...|differentiation/s...|Python| [[B@3c753c6]|[typing, Callable...|
|6d7c6cb29abb52fc2...|          gcd/gcd.py|Python|[[B@1a8cd0fd]|[gcd, a, int, b, ...|
|8ab978a56c5dcb239...|factorial/factori...|Python|[[B@485beb73]|[factorial, n, in...|
|e35a52f431feac4b7...|          abs/abs.py|Python|[[B@43b370e5]|[math, abs, x, re...|
|b2675a52ed6bfdfa9...|prime/is_prime_op...|Python|[[B@7a534236]|[math, is_prime, ...|
|51bdeff4494d60bb7...|euclidean/distanc...|Python| [[B@6246eb9]|[math, typing, Tu...|
|6d7c6cb29abb52fc2...|          gcd/gcd.py|Python|[[B@11b30d7d]|[gcd, a, int, b, ...|
|e35a52f431feac4b7...|          abs/abs.py|Python|[[B@495f63f6]|[math, abs, x, re...|
|8ab978a56c5dcb239...|factorial/factori...|Python|[[B@297dca19]|[factorial, n, in...|
+--------------------+--------------------+------+-------------+--------------------+
only showing top 20 rows
'''
```
