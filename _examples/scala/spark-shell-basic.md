## Basic example

In this example, the spark-shell is used to show a simple usage of the spark-api.

First, you can see how to import the package and instantiate and object that provide all the methods to manipulate the data retrieved from repositories.

The `api` object is used to filter repositories by `id`, get the `HEAD` references from the repositories and look for the commits in that references which contain the word `Initial` in their messages. Then a table is showed selecting the columns `repository_id`, `hash` and `message`.

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
