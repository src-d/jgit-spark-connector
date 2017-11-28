## Querying UASTs with XPath example

You can see in this example how to make queries using [XPath syntax](https://www.w3.org/TR/xpath/) to retrieve valuable information from the UASTs.

First we must use `extractUASTs` method to request to a [bblfsh daemon](https://github.com/bblfsh/bblfshd) the UASTs.

Then we can use the method `queryUAST` to get a result for the query we are formulating requesting tokens.  This method takes in three parameters, the query, the column which contains the UASTs and the column that will be generated with the result.

Finally, `extractTokens` method will generate a column `tokens` based on the previous generated column `result`.

Launch spark-shell, replacing `[version]` with the [latest engine version](http://search.maven.org/#search%7Cga%7C1%7Ctech.sourced):
```sh
$ spark-shell --packages "tech.sourced:engine:[version]"
```

Code:
```scala
import tech.sourced.engine._

val engine = Engine(spark, "/path/to/siva-files")
engine.getRepositories.getHEAD.getCommits.getTreeEntries.getBlobs.classifyLanguages.where('lang === "Python").extractUASTs.queryUAST("//*[@roleIdentifier]", "uast", "result").extractTokens("result", "tokens").select('path, 'lang, 'uast, 'tokens).show

/* Output:
+--------------------+------+-------------+--------------------+
|                path|  lang|         uast|              tokens|
+--------------------+------+-------------+--------------------+
|fibonacci/fibonac...|Python|[[B@466c4700]|[fibonacci, n, in...|
|          gcd/gcd.py|Python|[[B@22a4508c]|[math, gcd, a, in...|
|prime/is_prime_op...|Python|[[B@6772d8f3]|[math, is_prime, ...|
|factorial/factori...|Python| [[B@86bff75]|[math, factorial,...|
|   prime/is_prime.py|Python|[[B@2c1bed3f]|[is_prime, n, int...|
|pythagorean_tripl...|Python|[[B@2cbbf800]|[typing, List, ty...|
|prime/is_prime_op...|Python|[[B@5d7f1824]|[math, random, RA...|
|prime/is_prime_op...|Python| [[B@ab8c4a9]|[math, is_prime_o...|
|          gcd/gcd.py|Python|[[B@7939b2d4]|[math, gcd, a, in...|
|gcd/gcd_optimal_e...|Python| [[B@a313e0b]|[math, gcd_optima...|
|differentiation/s...|Python|[[B@2faab951]|[typing, Callable...|
|differentiation/s...|Python|[[B@637bad81]|[typing, Callable...|
|          gcd/gcd.py|Python|[[B@57601c28]|[gcd, a, int, b, ...|
|factorial/factori...|Python|[[B@5422a1a9]|[factorial, n, in...|
|          abs/abs.py|Python|[[B@2e38fa4d]|[math, abs, x, re...|
|prime/is_prime_op...|Python|[[B@10914dae]|[math, is_prime, ...|
|euclidean/distanc...|Python|[[B@47c782c8]|[math, typing, Tu...|
|          gcd/gcd.py|Python| [[B@6a94c70]|[gcd, a, int, b, ...|
|          abs/abs.py|Python|[[B@6faa347a]|[math, abs, x, re...|
|factorial/factori...|Python|[[B@754ce81c]|[factorial, n, in...|
+--------------------+------+-------------+--------------------+
only showing top 20 rows
*/
```
