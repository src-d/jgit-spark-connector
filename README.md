# spark-api [![Build Status](https://travis-ci.org/src-d/spark-api.svg?branch=master)](https://travis-ci.org/src-d/spark-api)


# TL;DR

Install `spark`:

```bash
$ cd /tmp && wget http://ftp.cixug.es/apache/spark/spark-2.2.0/spark-2.2.0-bin-hadoop2.7.tgz
$ tar -C ~/ -xvzf spark-2.2.0-bin-hadoop2.7.tgz
$ export SCALA_HOME=$HOME/spark-2.2.0-bin-hadoop2.7/bin
```

Run `spark-shell` with `spark-api`:

```bash
$ $SPARKHOME/bin/spark-shell --packages "com.github.src-d:spark-api:master-SNAPSHOT" --repositories "https://jitpack.io"
```

Run `pyspark` with `spark-api`:

```bash
$ $SPARK_HOME/bin/pyspark --repositories "https://jitpack.io"  --packages "tech.sourced:spark-api:master-SNAPSHOT"
```


# Pre-requests

## Spark Installation

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

or just set SPARK_HOME and run it as following:

```bash
$ export SPARK_HOME=$HOME/spark-2.2.0-bin-hadoop2.7
$ $SPARK_HOME/bin/spark-shell
```

# Examples of API usage

## pySpark

### Local mode

Install python-wrappers is necessary to use spark-api from pyspark:

``` bash
$ pip install <spark-api>
```

Then you should point to the remote repository where spark-api is hosted and provide the maven coordinates:
```bash
$ $SPARK_HOME/bin/pyspark --repositories "https://jitpack.io"  --packages "tech.sourced:spark-api:0.1.0-SNAPSHOT"
```

### Cluster mode

Install spark-api wrappers as in local mode:
```bash
$ sudo pip install -e 'git+https://github.com/erizocosmico/spark-api.git@feature/python-wrapper#egg=spark-api&subdirectory=python'
```

Then you should package and compress with `zip tool`  the python-wrappers to provide pyspark with it. It's required to distribute the code among the nodes of the cluster.

```bash
$ zip <path-to-installed-package> ./spark-api.zip
$ $SPARK_HOME/bin/pyspark <same-args-as-local-plus> --py-files ./spark-api.zip
```

### pySpark API usage

Run pyspark as explained before to start using spark-api:

```bash
$ $SPARK_HOME/bin/pyspark --packages com.github.src-d:spark-api:master-SNAPSHOT --repositories https://jitpack.ios
Welcome to

   spark version 2.2.0

Using Python version 3.6.2 (default, Jul 20 2017 03:52:27)
SparkSession available as 'spark'.
>>> from sourced.spark import API as SparkAPI
>>> from pyspark.sql import SparkSession
>>>
>>> spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
>>> api = SparkAPI(spark, '/path/to/siva/files')
>>> api.repositories.filter("id = 'github.com/mawag/faq-xiyoulinux'").references.filter("name = 'refs/heads/HEAD'").show()
+--------------------+---------------+--------------------+
|       repository_id|           name|                hash|
+--------------------+---------------+--------------------+
|github.com/mawag/...|refs/heads/HEAD|fff7062de8474d10a...|
+--------------------+---------------+--------------------+


```

## Scala API

For the moment, `spark-api`  can only be installed from [jitpack](https://jitpack.io) (coming soon availabe on maven central), so you should be able to run the `spark-shell` with `spark-api` as a required dependency in the following way:

```bash
$ spark-shell --packages com.github.src-d:spark-api:master-SNAPSHOT --repositories https://jitpack.io
```

To start using spark-api from the shell you must import `spark-api` package and Implicits object from it:

```bash
scala> import tech.sourced.api.Implicits_
import tech.sourced.api.Implicits_
```

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
