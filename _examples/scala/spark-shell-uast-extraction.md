```bash
$ spark-shell --packages com.github.src-d:spark-api:master-SNAPSHOT --repositories https://jitpack.io
scala> import tech.sourced.api._
import tech.sourced.api._

scala> val api = SparkAPI(spark, "/path/to/siva-files")

scala> val exampleDf = api.getRepositories.filter('id === "github.com/mingrammer/funmath.git").getHEAD.getFiles.extractUASTs.select('name, 'path, 'uast).where('uast.isNotNull)

scala> exampleDf.show

+---------------+--------------------+--------------------+
|           name|                path|                uast|
+---------------+--------------------+--------------------+
|refs/heads/HEAD|          .gitignore|                  []|
|refs/heads/HEAD|         .travis.yml|                  []|
|refs/heads/HEAD|             LICENSE|                  []|
|refs/heads/HEAD|           README.md|                  []|
|refs/heads/HEAD|          abs/abs.py|[0A 06 4D 6F 64 7...|
|refs/heads/HEAD|differentiation/s...|[0A 06 4D 6F 64 7...|
|refs/heads/HEAD|euclidean/distanc...|[0A 06 4D 6F 64 7...|
|refs/heads/HEAD|factorial/factori...|[0A 06 4D 6F 64 7...|
|refs/heads/HEAD|factorial/factori...|[0A 06 4D 6F 64 7...|
|refs/heads/HEAD|fibonacci/fibonac...|[0A 06 4D 6F 64 7...|
|refs/heads/HEAD|fibonacci/fibonac...|[0A 06 4D 6F 64 7...|
|refs/heads/HEAD|fibonacci/fibonac...|[0A 06 4D 6F 64 7...|
|refs/heads/HEAD|          gcd/gcd.py|[0A 06 4D 6F 64 7...|
|refs/heads/HEAD|gcd/gcd_optimal_e...|[0A 06 4D 6F 64 7...|
|refs/heads/HEAD|          lcm/lcm.py|[0A 06 4D 6F 64 7...|
|refs/heads/HEAD|lcm/lcm_optimal_e...|[0A 06 4D 6F 64 7...|
|refs/heads/HEAD|   prime/is_prime.py|[0A 06 4D 6F 64 7...|
|refs/heads/HEAD|prime/is_prime_im...|[0A 06 4D 6F 64 7...|
|refs/heads/HEAD|prime/is_prime_op...|[0A 06 4D 6F 64 7...|
|refs/heads/HEAD| prime/next_prime.py|[0A 06 4D 6F 64 7...|
+---------------+--------------------+--------------------+
only showing top 20 rows

```
