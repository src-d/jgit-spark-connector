## Classifying languages and extracting UASTs example

The combined usage of both, `classifyLanguages` and `extractUASTs` methods, has the advantage that doesn't rely the language detection task on the [bblfsh server](https://github.com/bblfsh/server) , so you can save some time.

To do that, you just have to call  `extractUASTs` on a Dataframe where previously, `classifyLanguages` was used.

Launch spark-shell, replacing `[version]` with the [latest engine version](http://search.maven.org/#search%7Cga%7C1%7Ctech.sourced):
```sh
$ spark-shell --packages "tech.sourced:engine:[version]"
```

Code:
```scala
import tech.sourced.engine._

val engine = Engine(spark, "/path/to/siva-files")
engine.getRepositories.getHEAD.getCommits.getTreeEntries.getBlobs.classifyLanguages.extractUASTs.select('blob_id, 'path, 'lang, 'uast).show

/* Output:
+--------------------+--------------------+--------+-------------+
|             blob_id|                path|    lang|         uast|
+--------------------+--------------------+--------+-------------+
|ff4fa0794274a7ffb...|fibonacci/fibonac...|  Python|[[B@62f37a44]|
|7268016814b8ab7bc...|          gcd/gcd.py|  Python|[[B@7c0368da]|
|25dbfff34dcc8d252...|           README.md|Markdown|           []|
|b2675a52ed6bfdfa9...|prime/is_prime_op...|  Python|[[B@7fa8bfe4]|
|63bd495dce1d53092...|factorial/factori...|  Python|[[B@3cad2dd4]|
|bf17d9730e43f5697...|         .travis.yml|    YAML|           []|
|a697a655a7bfd6ba1...|   prime/is_prime.py|  Python|[[B@45f5415f]|
|76052f368f4c9c8de...|pythagorean_tripl...|  Python|[[B@22d7a483]|
|3be2253ba2e871d3b...|prime/is_prime_op...|  Python|[[B@18ba78a2]|
|1ec7f95f8be7bf4f3...|prime/is_prime_op...|  Python|[[B@4dac25ec]|
|7268016814b8ab7bc...|          gcd/gcd.py|  Python|[[B@223c6abf]|
|793b6e21f2eebe900...|gcd/gcd_optimal_e...|  Python|[[B@3dd021c7]|
|4d3617f27e277e4b5...|differentiation/s...|  Python|[[B@76e431b7]|
|4d3617f27e277e4b5...|differentiation/s...|  Python|[[B@5a4bf9c2]|
|6d7c6cb29abb52fc2...|          gcd/gcd.py|  Python|[[B@1be309a6]|
|8ab978a56c5dcb239...|factorial/factori...|  Python|[[B@2781dd04]|
|e35a52f431feac4b7...|          abs/abs.py|  Python|[[B@70bf39ca]|
|b2675a52ed6bfdfa9...|prime/is_prime_op...|  Python|[[B@753f5bf6]|
|51bdeff4494d60bb7...|euclidean/distanc...|  Python|[[B@7612c2ce]|
|6d7c6cb29abb52fc2...|          gcd/gcd.py|  Python|[[B@5f5248f5]|
+--------------------+--------------------+--------+-------------+
only showing top 20 rows
*/
```
