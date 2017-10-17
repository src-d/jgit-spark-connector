## Classifying languages example

This example uses the spark-shell to show how to classify files by their language with `classifyLanguages`.

Making use of the `engine` object, it filters repositories by `id` to get all files from the `HEAD` references from them. After that, a call to `classifyLanguages` function detects the language for each file to show them in the aggregated column `lang` beside the selected columns `repository_id`, `file_hash` and `path`.

```bash
$ spark-shell --packages com.github.src-d:engine:master-SNAPSHOT --repositories https://jitpack.io
scala> import tech.sourced.engine._
import tech.sourced.engine._

scala> val engine = Engine(spark, "/path/to/siva-files")

scala> engine.getRepositories.filter('id === "github.com/mingrammer/funmath.git").getHEAD.getFiles.classifyLanguages.select('repository_id, 'file_hash, 'path, 'lang).show
+--------------------+--------------------+--------------------+--------+
|       repository_id|           file_hash|                path|    lang|
+--------------------+--------------------+--------------------+--------+
|github.com/mingra...|73fe61e5132f518f7...|          .gitignore|    null|
|github.com/mingra...|bf17d9730e43f5697...|         .travis.yml|    YAML|
|github.com/mingra...|524518a5bc6517f35...|             LICENSE|    Text|
|github.com/mingra...|8a9ea646b7936344d...|           README.md|Markdown|
|github.com/mingra...|e35a52f431feac4b7...|          abs/abs.py|  Python|
|github.com/mingra...|4d3617f27e277e4b5...|differentiation/s...|  Python|
|github.com/mingra...|e543efaa2472ca990...|euclidean/distanc...|  Python|
|github.com/mingra...|8ab978a56c5dcb239...|factorial/factori...|  Python|
|github.com/mingra...|bc9f129c7240b473c...|factorial/factori...|  Python|
|github.com/mingra...|ff4fa0794274a7ffb...|fibonacci/fibonac...|  Python|
|github.com/mingra...|26f33ebf9fc10ade0...|fibonacci/fibonac...|  Python|
|github.com/mingra...|34ec07bb8fd51505f...|fibonacci/fibonac...|  Python|
|github.com/mingra...|6d7c6cb29abb52fc2...|          gcd/gcd.py|  Python|
|github.com/mingra...|969cca02388dd9339...|gcd/gcd_optimal_e...|  Python|
|github.com/mingra...|efbab3fd5b65a8808...|          lcm/lcm.py|  Python|
|github.com/mingra...|f6e3b5434694dec73...|lcm/lcm_optimal_e...|  Python|
|github.com/mingra...|ff24547f19fb5afc2...|   prime/is_prime.py|  Python|
|github.com/mingra...|0b21309064736ec48...|prime/is_prime_im...|  Python|
|github.com/mingra...|70e8349ebddde4fb8...|prime/is_prime_op...|  Python|
|github.com/mingra...|c966f781ae72efe1b...| prime/next_prime.py|  Python|
+--------------------+--------------------+--------------------+--------+
only showing top 20 rows

```
