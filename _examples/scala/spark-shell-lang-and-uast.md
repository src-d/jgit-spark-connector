## Classifying languages and extracting UASTs example

The combined usage of both, `classifyLanguages` and `extractUASTs` methods, has the advantage that doesn't rely the language detection task on the [bblfsh server](https://github.com/bblfsh/server) , so you can save some time.

To do that, you just have to call  `extractUASTs` on a Dataframe where previously, `classifyLanguages` was used.

```bash
$ spark-shell --packages com.github.src-d:engine:master-SNAPSHOT --repositories https://jitpack.io
scala> import tech.sourced.engine._
import tech.sourced.engine._

scala> val engine = Engine(spark, "/path/to/siva-files")

scala> engine.getRepositories.getHEAD.getFiles.classifyLanguages.extractUASTs.select('repository_id, 'path, 'lang, 'uast).show

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
