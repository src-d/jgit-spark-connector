## Classifying languages and extracting UASTs example

The combined usage of both, `classify_languages()` and `extract_uasts()` methods, has the advantage that doesn't rely the language detection task on the [bblfsh server](https://github.com/bblfsh/server) , so you can save some time.

To do that, you just have to call  `extract_uasts()` on a Dataframe where previously, `classify_languages()` was used.

```bash
$ pyspark --packages com.github.src-d:engine:master-SNAPSHOT --repositories https://jitpack.io
>>> from sourced.engine import Engine
>>> engine = Engine(spark, '/path/to/siva-files')
>>> engine.repositories.references.head_ref.files.classify_languages().extract_uasts().select("path", "lang", "uast").show()

+--------------------+--------+--------------------+
|                path|    lang|                uast|
+--------------------+--------+--------------------+
|          .gitignore|    null|                  []|
|         .travis.yml|    YAML|                  []|
|             LICENSE|    Text|                  []|
|           README.md|Markdown|                  []|
|          abs/abs.py|  Python|[0A 06 4D 6F 64 7...|
|differentiation/s...|  Python|[0A 06 4D 6F 64 7...|
|euclidean/distanc...|  Python|[0A 06 4D 6F 64 7...|
|factorial/factori...|  Python|[0A 06 4D 6F 64 7...|
|factorial/factori...|  Python|[0A 06 4D 6F 64 7...|
|fibonacci/fibonac...|  Python|[0A 06 4D 6F 64 7...|
|fibonacci/fibonac...|  Python|[0A 06 4D 6F 64 7...|
|fibonacci/fibonac...|  Python|[0A 06 4D 6F 64 7...|
|          gcd/gcd.py|  Python|[0A 06 4D 6F 64 7...|
|gcd/gcd_optimal_e...|  Python|[0A 06 4D 6F 64 7...|
|          lcm/lcm.py|  Python|[0A 06 4D 6F 64 7...|
|lcm/lcm_optimal_e...|  Python|[0A 06 4D 6F 64 7...|
|   prime/is_prime.py|  Python|[0A 06 4D 6F 64 7...|
|prime/is_prime_im...|  Python|[0A 06 4D 6F 64 7...|
|prime/is_prime_op...|  Python|[0A 06 4D 6F 64 7...|
| prime/next_prime.py|  Python|[0A 06 4D 6F 64 7...|
+--------------------+--------+--------------------+
only showing top 20 rows

```
