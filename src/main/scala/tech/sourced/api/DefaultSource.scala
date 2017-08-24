package tech.sourced.api

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

class DefaultSource extends RelationProvider with DataSourceRegister {

  override def shortName() = "git"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val table = parameters.getOrElse(DefaultSource.tableNameKey, throw new SparkException("parameter 'table' must be provided"))

    table match {
      case "repositories" =>
        MockRelation(sqlContext, sqlContext.createDataFrame(MockedData.repositories, Schema.repositories))
      case "references" =>
        MockRelation(sqlContext, sqlContext.createDataFrame(MockedData.references, Schema.references))
      case "commits" =>
        MockRelation(sqlContext, sqlContext.createDataFrame(MockedData.commits, Schema.commits))
      case "files" =>
        MockRelation(sqlContext, sqlContext.createDataFrame(MockedData.files, Schema.files))
      case other => throw new SparkException(s"table '$other' not supported")
    }
  }
}

object DefaultSource {
  val tableNameKey = "table"
  val pathKey = "path"
}

case class MockRelation(sqlContext: SQLContext, data: DataFrame) extends BaseRelation with PrunedFilteredScan {
  override def schema: StructType = data.schema

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {

    val projectResult = requiredColumns.length match {
      case l if l >= 2 => data.select(requiredColumns.head, requiredColumns.tail: _*)
      case 1 => data.select(requiredColumns.head)
      case 0 => data
    }

    println("==== COLUMNS ===")
    println(requiredColumns.mkString(","))

    // TODO not necessary for the demo, because spark also filter after the datasource filtering
    println("==== FILTERS ===")
    println(filters.mkString(","))

    projectResult.rdd
  }
}
