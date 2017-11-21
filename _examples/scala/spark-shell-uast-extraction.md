## Extracting UASTs example

In the example code below, you can take a look to how the `extractUASTs` method works.

From the `engine` object instantiated in the spark-shell, a bunch of blobs has been got filtering repositories by `id`, retrieving their `HEAD` references and requesting for them. Once we have that blobs, we can call `extractUASTs` which send the blobs to a [bblfsh server](https://github.com/bblfsh/server) to get back the UASTs.

Finally, the `blob_id`, file `path` and `uast` is showed on the table.

Launch spark-shell, replacing `[version]` with the [latest engine version](http://search.maven.org/#search%7Cga%7C1%7Ctech.sourced):
```sh
$ spark-shell --packages "tech.sourced:engine:[version]"
```

```scala
import tech.sourced.engine._

val engine = Engine(spark, "/path/to/siva-files")
val exampleDf = engine.getRepositories.filter('id === "github.com/mingrammer/funmath.git").getHEAD.getCommits.getTreeEntries.getBlobs.extractUASTs.select('blob_id, 'path, 'uast)

exampleDf.show

/* Output:
+--------------------+--------------------+-------------+
|             blob_id|                path|         uast|
+--------------------+--------------------+-------------+
|ff4fa0794274a7ffb...|fibonacci/fibonac...|[[B@5e53daf6]|
|7268016814b8ab7bc...|          gcd/gcd.py|[[B@65f08242]|
|25dbfff34dcc8d252...|           README.md|           []|
|b2675a52ed6bfdfa9...|prime/is_prime_op...|[[B@7d81ce6a]|
|63bd495dce1d53092...|factorial/factori...|[[B@4c903df9]|
|bf17d9730e43f5697...|         .travis.yml|           []|
|a697a655a7bfd6ba1...|   prime/is_prime.py| [[B@cd4caf7]|
|76052f368f4c9c8de...|pythagorean_tripl...|[[B@6d57bbbd]|
|3be2253ba2e871d3b...|prime/is_prime_op...|[[B@1ed6dae3]|
|1ec7f95f8be7bf4f3...|prime/is_prime_op...|[[B@53e45335]|
|7268016814b8ab7bc...|          gcd/gcd.py|[[B@79cda8cc]|
|793b6e21f2eebe900...|gcd/gcd_optimal_e...|[[B@29976e1b]|
|4d3617f27e277e4b5...|differentiation/s...| [[B@13ea808]|
|4d3617f27e277e4b5...|differentiation/s...|[[B@70323ee1]|
|6d7c6cb29abb52fc2...|          gcd/gcd.py|[[B@642d63e3]|
|8ab978a56c5dcb239...|factorial/factori...|[[B@76583ecb]|
|e35a52f431feac4b7...|          abs/abs.py| [[B@252b6e0]|
|b2675a52ed6bfdfa9...|prime/is_prime_op...|[[B@63f6557d]|
|51bdeff4494d60bb7...|euclidean/distanc...|[[B@6ccb009b]|
|6d7c6cb29abb52fc2...|          gcd/gcd.py|[[B@5b52d5af]|
+--------------------+--------------------+-------------+
only showing top 20 rows
*/
```
