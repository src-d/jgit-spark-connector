# Pre-compute repository metadata and save it to another DataSource

| Field | Value |
| --- | --- |
| ENIP | 1 |
| Title | Pre-compute repository metadata and save it to another DataSource |
| Author | Antonio Navarro |
| Status | Rejected |
| Created | 2017-11-14 |
| Updated | 2017-11-21 |
| Target version | - |

## Abstract

With this change we want to improve the performance of reading repositories metadata,
saving that metadata in other DataSource than GitDataSource.
It can be any of the already implemented ones (json,parquet,jdbc and so on).

## Rationale

Reading the content of siva files over and over again is not really performant.
With this ENIP we want a way to improve speed reading metadata (repositories, references, commits, and tree entries).

To do that,
we are going to add new methods on the api using the already existing methods on DataFrame API,
[reader][1] and [writer][2].

## Specification
To be able to register other datasource than GitDataSource, we should change a bit the way that we are geting the datasources to process commits, references, or blobs.

Actually we are registering datasources using `getDatasource` method:

```scala
/**
    * Returns a [[org.apache.spark.sql.DataFrame]] for the given table using the provided
    * [[org.apache.spark.sql.SparkSession]].
    *
    * @param table   name of the table
    * @param session spark session
    * @return dataframe for the given table
    */
  private[engine] def getDataSource(table: String, session: SparkSession): DataFrame =
    session.read.format("tech.sourced.engine.DefaultSource")
      .option("table", table)
      .load(session.sqlContext.getConf(repositoriesPathKey))
```

Instead of this, we can create a view using the SparkSession from several datasources:

```scala
/**
  * Creates a local temporary view using the given name. The lifetime of this
  * temporary view is tied to the [[SparkSession]] that was used to create this Dataset.
  *
  * @group basic
  * @since 2.0.0
  */
 def createOrReplaceTempView(viewName: String): Unit = withPlan {
   createTempViewCommand(viewName, replace = true, global = false)
 }
```

`createOrReplaceTempView` method will allow us to register tables at the engine instantiation with several datasources.
Then, from implicit DataFrame methods, we can do:

```scala
val commitsDf = df.sparkSession.table("commits")
```

Instead of:
```scala
val commitsDf = getDataSource("commits", df.sparkSession)
```

Then, the list of needed changes on the Engine API are:
- Initialize GitDataSource views at Engine initialization
- Add method `backMetadataToSource(options)` (name to decide) into the Engine API.
- Add method `fromMetadataSource(options)` (name to decide) into the Engine API.
That method will change all the default registered views to the specified DataSource.

We should check speed improvement with a substantial amount of repositories and several DataSources.

## Alternatives
Using the already existing Spark Dataframe API,
we can save that metadata.

Example:
```scala
repositoriesDf.write.bucketBy(100,"repository_url").parquet("repositories.parquet")
// or
repositoriesDf.write.jdbc(url, tableName, properties)

```

And then read it using `SparkSession.read` method.

## Impact

The actual Join Rule optimization for Git Datasources will not be applied.
That means,
if we do a Join between two jdbc datasources table,
the Join will be executed at Spark level,
doing a full scan on both jdbc tables.
That can works really well with small amount of repositories and siva files,
but if we want an Engine as scalable as Spark, we should avoid this kind of operations.

## References

[DataFrameReader API][1]

[DataFrameWriter API][2]

[1]: https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameReader
[2]: https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameWriter
