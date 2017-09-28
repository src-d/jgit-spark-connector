```bash
$ pyspark --packages com.github.src-d:spark-api:master-SNAPSHOT --repositories https://jitpack.io
>>> from sourced.spark import API as SparkAPI
>>> api = SparkAPI(spark, '/path/to/siva-files')
>>> api.repositories.references.head_ref.files.classify_languages().select("file_hash", "path", "lang").show()
+--------------------+--------------------+--------+
|           file_hash|                path|    lang|
+--------------------+--------------------+--------+
|73fe61e5132f518f7...|          .gitignore|    null|
|bf17d9730e43f5697...|         .travis.yml|    YAML|
|524518a5bc6517f35...|             LICENSE|    Text|
|8a9ea646b7936344d...|           README.md|Markdown|
|e35a52f431feac4b7...|          abs/abs.py|  Python|
|4d3617f27e277e4b5...|differentiation/s...|  Python|
|e543efaa2472ca990...|euclidean/distanc...|  Python|
|8ab978a56c5dcb239...|factorial/factori...|  Python|
|bc9f129c7240b473c...|factorial/factori...|  Python|
|ff4fa0794274a7ffb...|fibonacci/fibonac...|  Python|
|26f33ebf9fc10ade0...|fibonacci/fibonac...|  Python|
|34ec07bb8fd51505f...|fibonacci/fibonac...|  Python|
|6d7c6cb29abb52fc2...|          gcd/gcd.py|  Python|
|969cca02388dd9339...|gcd/gcd_optimal_e...|  Python|
|efbab3fd5b65a8808...|          lcm/lcm.py|  Python|
|f6e3b5434694dec73...|lcm/lcm_optimal_e...|  Python|
|ff24547f19fb5afc2...|   prime/is_prime.py|  Python|
|0b21309064736ec48...|prime/is_prime_im...|  Python|
|70e8349ebddde4fb8...|prime/is_prime_op...|  Python|
|c966f781ae72efe1b...| prime/next_prime.py|  Python|
+--------------------+--------------------+--------+
only showing top 20 rows

```
