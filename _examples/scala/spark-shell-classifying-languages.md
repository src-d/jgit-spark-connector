## Classifying languages example

This example uses the spark-shell to show how to classify blobs by their language with `classifyLanguages`.

Making use of the `engine` object, it filters repositories by `id` to get all blobs from the `HEAD` references from them. After that, a call to `classifyLanguages` function detects the language for each file to show them in the aggregated column `lang` beside the selected columns `blob_id` and `path`.

Launch spark-shell, replacing `[version]` with the [latest engine version](http://search.maven.org/#search%7Cga%7C1%7Ctech.sourced):
```sh
$ spark-shell --packages "tech.sourced:engine:[version]"
```

Code:
```scala
import tech.sourced.engine._

val engine = Engine(spark, "/path/to/siva-files")
engine.getRepositories.filter('id === "github.com/mingrammer/funmath.git").getHEAD.getCommits.getTreeEntries.getBlobs.classifyLanguages.select('blob_id, 'path, 'lang).show

/* Output:
+--------------------+--------------------+--------+
|             blob_id|                path|    lang|
+--------------------+--------------------+--------+
|ff4fa0794274a7ffb...|fibonacci/fibonac...|  Python|
|7268016814b8ab7bc...|          gcd/gcd.py|  Python|
|25dbfff34dcc8d252...|           README.md|Markdown|
|b2675a52ed6bfdfa9...|prime/is_prime_op...|  Python|
|63bd495dce1d53092...|factorial/factori...|  Python|
|bf17d9730e43f5697...|         .travis.yml|    YAML|
|a697a655a7bfd6ba1...|   prime/is_prime.py|  Python|
|76052f368f4c9c8de...|pythagorean_tripl...|  Python|
|3be2253ba2e871d3b...|prime/is_prime_op...|  Python|
|1ec7f95f8be7bf4f3...|prime/is_prime_op...|  Python|
|7268016814b8ab7bc...|          gcd/gcd.py|  Python|
|793b6e21f2eebe900...|gcd/gcd_optimal_e...|  Python|
|4d3617f27e277e4b5...|differentiation/s...|  Python|
|4d3617f27e277e4b5...|differentiation/s...|  Python|
|6d7c6cb29abb52fc2...|          gcd/gcd.py|  Python|
|8ab978a56c5dcb239...|factorial/factori...|  Python|
|e35a52f431feac4b7...|          abs/abs.py|  Python|
|b2675a52ed6bfdfa9...|prime/is_prime_op...|  Python|
|51bdeff4494d60bb7...|euclidean/distanc...|  Python|
|6d7c6cb29abb52fc2...|          gcd/gcd.py|  Python|
+--------------------+--------------------+--------+
only showing top 20 rows
*/
```
