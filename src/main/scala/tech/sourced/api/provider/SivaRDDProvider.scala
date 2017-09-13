package tech.sourced.api.provider

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.SparkContext
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD

import scala.collection.convert.decorateAsScala._
import scala.collection.concurrent

class SivaRDDProvider(sc: SparkContext) {
  private val sivaFilesRDD: concurrent.Map[String, RDD[PortableDataStream]] =
    new ConcurrentHashMap[String, RDD[PortableDataStream]]().asScala

  def get(path: String): RDD[PortableDataStream] =
    sivaFilesRDD.getOrElse(path, SivaRDDProvider.generateRDD(sc, path))
}

object SivaRDDProvider {
  var provider: SivaRDDProvider = _

  def apply(sc: SparkContext): SivaRDDProvider = {
    Option(provider).getOrElse({
      provider = new SivaRDDProvider(sc)
      provider
    })
  }

  private def generateRDD(sc: SparkContext, path: String): RDD[PortableDataStream] =
    sc.binaryFiles(path).map(_._2)
}
