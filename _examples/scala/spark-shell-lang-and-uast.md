```bash
$ spark-shell --packages com.github.src-d:spark-api:master-SNAPSHOT --repositories https://jitpack.io
scala> import tech.sourced.api._
import tech.sourced.api._

scala> val api = SparkAPI(spark, "/path/to/siva-files")

scala> api.getRepositories.getHEAD.getFiles.classifyLanguages.extractUASTs.select('repository_id, 'path, 'lang, 'uast).show

+--------------------+--------------------+--------+--------------------+
|       repository_id|                path|    lang|                uast|
+--------------------+--------------------+--------+--------------------+
|github.com/mingra...|          .gitignore|    null|                  []|
|github.com/mingra...|         .travis.yml|    YAML|                  []|
|github.com/mingra...|             LICENSE|    Text|                  []|
|github.com/mingra...|           README.md|Markdown|                  []|
|github.com/mingra...|          abs/abs.py|  Python|[0A 06 4D 6F 64 7...|
|github.com/mingra...|differentiation/s...|  Python|[0A 06 4D 6F 64 7...|
|github.com/mingra...|euclidean/distanc...|  Python|[0A 06 4D 6F 64 7...|
|github.com/mingra...|factorial/factori...|  Python|[0A 06 4D 6F 64 7...|
|github.com/mingra...|factorial/factori...|  Python|[0A 06 4D 6F 64 7...|
|github.com/mingra...|fibonacci/fibonac...|  Python|[0A 06 4D 6F 64 7...|
|github.com/mingra...|fibonacci/fibonac...|  Python|[0A 06 4D 6F 64 7...|
|github.com/mingra...|fibonacci/fibonac...|  Python|[0A 06 4D 6F 64 7...|
|github.com/mingra...|          gcd/gcd.py|  Python|[0A 06 4D 6F 64 7...|
|github.com/mingra...|gcd/gcd_optimal_e...|  Python|[0A 06 4D 6F 64 7...|
|github.com/mingra...|          lcm/lcm.py|  Python|[0A 06 4D 6F 64 7...|
|github.com/mingra...|lcm/lcm_optimal_e...|  Python|[0A 06 4D 6F 64 7...|
|github.com/mingra...|   prime/is_prime.py|  Python|[0A 06 4D 6F 64 7...|
|github.com/mingra...|prime/is_prime_im...|  Python|[0A 06 4D 6F 64 7...|
|github.com/mingra...|prime/is_prime_op...|  Python|[0A 06 4D 6F 64 7...|
|github.com/mingra...| prime/next_prime.py|  Python|[0A 06 4D 6F 64 7...|
+--------------------+--------------------+--------+--------------------+
only showing top 20 rows

```
