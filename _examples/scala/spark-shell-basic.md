```bash
$ spark-shell --packages com.github.src-d:spark-api:master-SNAPSHOT --repositories https://jitpack.io
scala> import tech.sourced.api._
import tech.sourced.api._

scala> val api = SparkAPI(spark, "/path/to/siva-files")

scala> api.getRepositories.filter('id === "github.com/mawag/faq-xiyoulinux").
     | getReferences.filter('name === "refs/heads/HEAD").
     | getCommits.filter('message.contains("Initial")).
     | select('repository_id, 'hash, 'message).
     | show

     +--------------------------------+-------------------------------+--------------------+
     |                 repository_id|                                hash|          message|
     +--------------------------------+-------------------------------+--------------------+
     |github.com/mawag/...|fff7062de8474d10a...|Initial commit|
     +--------------------------------+-------------------------------+--------------------+

```
