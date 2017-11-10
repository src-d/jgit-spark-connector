# engine [![Build Status](https://travis-ci.org/src-d/engine.svg?branch=master)](https://travis-ci.org/src-d/engine) [![codecov](https://codecov.io/gh/src-d/engine/branch/master/graph/badge.svg)](https://codecov.io/gh/src-d/engine) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/tech.sourced/engine/badge.svg)](https://maven-badges.herokuapp.com/maven-central/tech.sourced/engine)

**engine** is a library for running scalable data retrieval pipelines that process any number of Git repositories for source code analysis.

It is written in Scala and built on top of Apache Spark to enable rapid construction of custom analysis pipelines and processing large number of Git repositories stored in HDFS in [Siva file format](https://github.com/src-d/go-siva). It is accessible both via Scala and Python Spark APIs, and capable of running on large-scale distributed clusters.

Current implementation combines:
 - [src-d/enry](https://github.com/src-d/enry) to detect programming language of every file
 - [bblfsh/client-scala](https://github.com/bblfsh/client-scala) to parse every file to UAST
 - [src-d/siva-java](https://github.com/src-d/siva-java) for reading Siva files in JVM
 - [apache/spark](https://github.com/apache/spark) to extend DataFrame API
 - [eclipse/jgit](https://github.com/eclipse/jgit) for working with Git .pack files


# Quick-start

Look for the latest [**engine** version](http://search.maven.org/#search%7Cga%7C1%7Ctech.sourced), and then replace in the command where `[version]` is showed:

```bash
$ wget "https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=spark/spark-2.2.0/spark-2.2.0-bin-hadoop2.7.tgz" -O spark-2.2.0-bin-hadoop2.7.tgz
$ tar -xzf spark-2.2.0-bin-hadoop2.7.tgz; cd spark-2.2.0-bin-hadoop2.7
$ ./bin/spark-shell --packages "tech.sourced:engine:[version]"

# or
$ ./bin/pyspark --packages "tech.sourced:engine:[version]"
```

Run [bblfsh daemon](https://github.com/bblfsh/bblfshd):

    docker create --rm --name bblfshd --privileged -p 9432:9432 -v /var/lib/bblfshd:/var/lib/bblfshd bblfsh/bblfshd:v2.1.2

Then, execute the container:

    docker start bblfshd

Install bblfsh drivers:

    docker exec -it bblfshd bblfshctl driver install --all

# Pre-requisites

## Apache Spark Installation

First, you need to download [Apache Spark](https://spark.apache.org/) somewhere on your machine:

```bash
$ cd /tmp && wget "https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=spark/spark-2.2.0/spark-2.2.0-bin-hadoop2.7.tgz" -O spark-2.2.0-bin-hadoop2.7.tgz
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

## bblfsh

If you want to be able to use the UAST extraction features **engine** provides, you must run a [bblfsh daemon](https://github.com/bblfsh/bblfshd). You can do it easily with docker:

    docker run --rm --name bblfshd --privileged -p 9432:9432 -v /var/lib/bblfshd:/var/lib/bblfshd bblfsh/bblfshd:v2.1.2

Then you need to install bblfsh drivers to parse different languages, you should do this the first time you run the [bblfsh daemon](https://github.com/bblfsh/bblfshd):

    docker exec -it bblfshd bblfshctl driver install --all

You should be able to see the installed drivers running:

    docker exec -it bblfshd bblfshctl driver list

# Examples of engine usage

**engine** is available on [maven central](https://search.maven.org/#search%7Cga%7C1%7Ctech.sourced.engine). To add it to your project as a dependency,

For projects managed by [maven](https://maven.apache.org/) add the following to your `pom.xml`:

```xml
<dependency>
    <groupId>tech.sourced</groupId>
    <artifactId>engine</artifactId>
    <version>[version]</version>
</dependency>
```

For [sbt](http://www.scala-sbt.org/) managed projects add the dependency:

    libraryDependencies += "tech.sourced" % "engine" % "[version]"

In both cases, replace `[version]` with the [latest engine version](http://search.maven.org/#search%7Cga%7C1%7Ctech.sourced)

## pyspark

### Local mode

Install python-wrappers is necessary to use **engine** from pyspark:

``` bash
$ pip install sourced-engine
```

Then you should provide the **engine's** maven coordinates to the pyspark's shell:
```bash
$ $SPARK_HOME/bin/pyspark --packages "tech.sourced:engine:[version]"
```
Replace `[version]` with the [latest engine version](http://search.maven.org/#search%7Cga%7C1%7Ctech.sourced)

### Cluster mode

Install **engine** wrappers as in local mode:
```bash
$ pip install -e sourced-engine
```

Then you should package and compress with `zip`  the python wrappers to provide pyspark with it. It's required to distribute the code among the nodes of the cluster.

```bash
$ zip <path-to-installed-package> ./sourced-engine.zip
$ $SPARK_HOME/bin/pyspark <same-args-as-local-plus> --py-files ./sourced-engine.zip
```

### pyspark API usage

Run pyspark as explained before to start using the engine, replacing `[version]` with the [latest engine version](http://search.maven.org/#search%7Cga%7C1%7Ctech.sourced):

```bash
$ $SPARK_HOME/bin/pyspark --packages "tech.sourced:engine:[version]"
Welcome to

   spark version 2.2.0

Using Python version 3.6.2 (default, Jul 20 2017 03:52:27)
SparkSession available as 'spark'.
>>> from sourced.engine import Engine
>>> engine = Engine(spark, '/path/to/siva/files')
>>> engine.repositories.filter('id = "github.com/mingrammer/funmath.git"').references.filter("name = 'refs/heads/HEAD'").show()
+--------------------+---------------+--------------------+
|       repository_id|           name|                hash|
+--------------------+---------------+--------------------+
|github.com/mingra...|refs/heads/HEAD|290440b64a73f5c7e...|
+--------------------+---------------+--------------------+

```

## Scala API usage

You must provide **engine** as a dependency in the following way, replacing `[version]` with the [latest engine version](http://search.maven.org/#search%7Cga%7C1%7Ctech.sourced):

```bash
$ spark-shell --packages "tech.sourced:engine:[version]"
```

To start using **engine** from the shell you must import everything inside the `tech.sourced.engine` package (or, if you prefer, just import `Engine` and `EngineDataFrame` classes):

```bash
scala> import tech.sourced.engine._
import tech.sourced.engine._
```

Now, you need to create an instance of `Engine` and give it the spark session and the path of the directory containing the siva files:

```bash
scala> val engine = Engine(spark, "/path/to/siva-files")
```

Then, you will be able to perform queries over the repositories:

```bash
scala> engine.getRepositories.filter('id === "github.com/mawag/faq-xiyoulinux").
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

# Playing around with **engine** on Jupyter

You can launch our docker container which contains some Notebooks examples just running:

    docker run --name engine-jupyter --rm -it -p 8888:8888 -v $(pwd)/path/to/siva-files:/repositories --link bblfshd:bblfshd srcd/engine-jupyter

You must have some siva files in local to mount them on the container replacing the path `$(pwd)/path/to/siva-files`. You can get some siva-files from the project [here](https://github.com/src-d/engine/tree/master/examples/siva-files).

You should have a [bblfsh daemon](https://github.com/bblfsh/bblfshd) container running to link the jupyter container (see Pre-requisites).

When the `engine-jupyter` container starts it will show you an URL that you can open in your browser.

# Development

## Build fatjar

Build the fatjar is needed to build the docker image that contains the jupyter server,  or test changes in spark-shell just passing the jar with `--jars` flag:

```bash
$ make build
```

It leaves the fatjar in `target/scala-2.11/engine-uber.jar`

## Build and run docker to get a Jupyter server

To build an image with the last built of the project:

```bash
$ make docker-build
```

Notebooks under examples folder will be included on the image.

To run a container with the Jupyter server:

```bash
$ make docker-run
```

Before run the jupyter container you must run a [bblfsh daemon](https://github.com/bblfsh/bblfshd):

```bash
$ make docker-bblfsh
```

If it's the first time you run the [bblfsh daemon](https://github.com/bblfsh/bblfshd), you must install the drivers:

```bash
$ make docker-bblfsh-install-drivers
```

To see installed drivers:

```bash
$ make docker-bblfsh-list-drivers
```

To remove the development jupyter image generated:

```bash
$ make docker-clean
```

## Run tests

**engine** uses [bblfsh](https://github.com/bblfsh) so you need an instance of a bblfsh server running:

```bash
$ make docker-bblfsh
```

To run tests:
```bash
$ make test
```

To run tests for python wrapper:

```bash
$ cd python
$ make test
```

### Windows support

There is no windows support in enry-java or bblfsh's client-scala right now, so all the language detection and UAST features are not available for the windows platform.

# License

Apache License Version 2.0, see [LICENSE](LICENSE)
