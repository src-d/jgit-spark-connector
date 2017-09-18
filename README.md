# spark-api


Installation
-----------------

Firstly, you need to download `spark` somewhere on your machine:

```bash
$ cd /tmp && wget http://ftp.cixug.es/apache/spark/spark-2.2.0/spark-2.2.0-bin-hadoop2.7.tgz
```
The Apache Software Foundation suggests you the better mirror where you can download `spark` from. If you wish to take a look and find the best option in your case, you can [do it here](https://www.apache.org/dyn/closer.lua/spark/spark-2.2.0/spark-2.2.0-bin-hadoop2.7.tgz).

Then you must extract spark from the downloaded tar file:

```bash
$ tar -C ~/ -xvzf spark-2.2.0-bin-hadoop2.7.tgz
```
Binaries and scripts to run `spark` are located in spark-2.2.0-bin-hadoop2.7/bin, so maybe you would like to add it to your `PATH`:

```bash
$ export PATH=$PATH:$HOME/spark-2.2.0-bin-hadoop2.7/bin
```


Run spark-api
---------------------

By the moment, `spark-api`  can be downloaded from [jitpack](https://jitpack.io), so you should be able to run the `spark-shell` with `spark-api` as a required dependency it the following way:

```bash
$ spark-shell --packages com.github.src-d:spark-api:master-SNAPSHOT --repositories https://jitpack.io
```

To start using spark-api from the shell you must import `spark-api` package and Implicits object from it:

```bash
scala> import tech.sourced.api._
import tech.sourced.api._

scala> import Implicits._
import Implicits._
```


Example of use
-----------------------

To load siva files as the data source, you have to point to the directory that contains them:

```bash
scala> spark.sqlContext.setConf("tech.sourced.api.repositories.path", "/path/to/siva-files")
```

Then you will be able to perform queries over the repositories:

```bash
scala> spark.getRepositories.filter('id === "github.com/mawag/faq-xiyoulinux").
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
