package tech.sourced.api.provider

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.SparkContext
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD

import scala.collection.convert.decorateAsScala._
import scala.collection.concurrent

/**
  * Provides an RDD of siva files.
  *
  * @param sc Spark Context
  */
class SivaRDDProvider(sc: SparkContext) {
  private val sivaFilesRDD: concurrent.Map[String, RDD[PortableDataStream]] =
    new ConcurrentHashMap[String, RDD[PortableDataStream]]().asScala

  /**
    * Generates an RDD of siva files at the given path.
    *
    * @param path Path where the siva files are stored.
    * @return RDD of siva files
    */
  def get(path: String): RDD[PortableDataStream] =
    sivaFilesRDD.getOrElse(path, SivaRDDProvider.generateRDD(sc, path))
}

/**
  * Provides some utility methods for [[SivaRDDProvider]] class.
  * Acts as a singleton for getting an unique instance of [[SivaRDDProvider]]s, so the
  * recommended way of using said class is using this companion object.
  */
object SivaRDDProvider {
  /** The singleton Siva RDD provider. */
  var provider: SivaRDDProvider = _

  /**
    * Returns the provider instance and creates one if none has been created yet.
    *
    * @param sc Spark Context
    * @return Siva RDD provider
    */
  def apply(sc: SparkContext): SivaRDDProvider = {
    Option(provider).getOrElse({
      provider = new SivaRDDProvider(sc)
      provider
    })
  }

  /**
    * Generates an RDD of [[PortableDataStream]] with the files at the given path.
    *
    * @param sc   Spark Context
    * @param path path to get the files from
    * @return generated RDD
    */
  private def generateRDD(sc: SparkContext, path: String): RDD[PortableDataStream] =
    sc.binaryFiles(path).map(_._2)
}
