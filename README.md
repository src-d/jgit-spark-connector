# spark-api [![Build Status](https://travis-ci.org/src-d/spark-api.svg?branch=master)](https://travis-ci.org/src-d/spark-api)

High-level Spark API is for running scalable data retrieval pipelines that process and manipulate any number of code repositories for source code analysis. Written mostly in Scala, it aims to be robust, friendly and flexible: it is built on top of Apache Spark, accessible both via Scala and Python Spark APIs, and capable of running on large-scale distributed clusters over petabytes of data.

# Quickstart

```bash
$ wget "https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=spark/spark-2.2.0/spark-2.2.0-bin-hadoop2.7.tgz"
$ tar -xzf spark-2.2.0-bin-hadoop2.7.tgz; cd spark-2.2.0-bin-hadoop2.7
$ bin/spark-shell --packages "com.github.src-d:spark-api:master-SNAPSHOT" --repositories "https://jitpack.io"

# or
$ bin/pyspark --repositories "https://jitpack.io"  --packages "com.github.src-d:spark-api:master-SNAPSHOT"
```


# Pre-requisites

## Apache Spark Installation

First, you need to download [Apache Spark](https://spark.apache.org/) somewhere on your machine:

```bash
$ cd /tmp && wget https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=spark/spark-2.2.0/spark-2.2.0-bin-hadoop2.7.tgz
```
The Apache Software Foundation suggests you the better mirror where you can download `Spark` from. If you wish to take a look and find the best option in your case, you can [do it here](https://www.apache.org/dyn/closer.lua/spark/spark-2.2.0/spark-2.2.0-bin-hadoop2.7.tgz).

Then you must extract `Spark` from the downloaded tar file:

```bash
$ tar -C ~/ -xvzf spark-2.2.0-bin-hadoop2.7.tgz
```
Binaries and scripts to run `Spark` are located in spark-2.2.0-bin-hadoop2.7/bin, so maybe you would like to add it to your `PATH`:

```bash
$ export PATH=$PATH:$HOME/spark-2.2.0-bin-hadoop2.7/bin
```

or just set `SPARK_HOME` and run it as following:

```bash
$ export SPARK_HOME=$HOME/spark-2.2.0-bin-hadoop2.7
$ $SPARK_HOME/bin/spark-shell
```

# Examples of API usage

## pyspark

### Local mode

Install python-wrappers is necessary to use spark-api from pyspark:

``` bash
$ pip install  'git+https://github.com/src-d/spark-api.git@master#egg=spark-api&subdirectory=python'
```

Then you should point to the remote repository where spark-api is hosted and provide the maven coordinates:
```bash
$ $SPARK_HOME/bin/pyspark --repositories "https://jitpack.io"  --packages "tech.sourced:spark-api:0.1.0-SNAPSHOT"
```

### Cluster mode

Install spark-api wrappers as in local mode:
```bash
$ pip install -e 'git+https://github.com/src-d/spark-api.git@master#egg=spark-api&subdirectory=python'
```

Then you should package and compress with `zip`  the python wrappers to provide pyspark with it. It's required to distribute the code among the nodes of the cluster.

```bash
$ zip <path-to-installed-package> ./spark-api.zip
$ $SPARK_HOME/bin/pyspark <same-args-as-local-plus> --py-files ./spark-api.zip
```

**NOTE:** right now, all the cluster nodes must be running the same operating system, since jar is not yet uploaded to maven central containing the native binaries for MacOS and Linux.

### pyspark API usage

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

For the moment, `spark-api`  can only be installed from [jitpack](https://jitpack.io) (will be available from Maven Central soon), so you should be able to run the `spark-shell` with `spark-api` as a required dependency in the following way:

```bash
$ spark-shell --packages com.github.src-d:spark-api:master-SNAPSHOT --repositories https://jitpack.io
```

To start using spark-api from the shell you must import everything inside the `tech.sourced.api` package (or, if you prefer, just import `SparkAPI` and `ApiDataFrame` classes):

```bash
scala> import tech.sourced.api._
import tech.sourced.api._
```

Now, you need to create an instance of `SparkAPI` and give it the spark session and the path of the directory containing the siva files:

```bash
scala> val api = SparkAPI(spark, "/path/to/siva-files")
```

Then, you will be able to perform queries over the repositories:

```bash
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

# Development

## Build fatjar

Build the fatjar is needed to build the docker image that contains the jupyter server,  or test changes in spark-shell just passing the jar with `--jars` flag:

```bash
$ ./sbt assembly
```

It leaves the fatjar in `target/scala-2.11/spark-api-uber.jar`

## Build and run docker to get a Jupyter server

```bash
$ docker -t spark-api-jupyter .
$ docker run -p 8888:8888 -v /path/to/siva-files:/repositories spark-api-jupyter
```

Notebooks under examples folder will be included on the image.

## Run tests

spark-api uses [bblfsh](https://github.com/bblfsh) so you need an instance of a bblfsh server running, and you can get one so easy with docker:

```bash
docker run -d --privileged -p 9432:9432 --name bblfsh bblfsh/server bblfsh server --log-level debug
```

To run tests:
```bash
$ ./sbt tests
```

To run tests for python wrapper:

```bash
$ cd python
$ python -m unittest discover -v
```

## enry

spark-api uses [java bindings](https://github.com/src-d/enry/tree/master/java) from [enry](https://github.com/src-d/enry) but for the moment you must build them manually and publish them in your local repository.

```bash
$ git clone https://github.com/src-d/enry.git; cd enry/java
$ make package && ./sbt publish-local
```

### Windows support

There is no windows support in enry-java right now, so all the language detection features also have no windows support.

# License

Apache License Version 2.0, see [LICENSE](https://github.com/src-d/spark-api/blob/master/LICENSE)
