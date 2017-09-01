package tech.sourced.api.provider

import org.apache.spark.SparkContext
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD

import scala.collection.mutable

class SivaRDDProvider(sc: SparkContext) {
  private val sivaFilesRDD: mutable.Map[String, RDD[PortableDataStream]] = mutable.Map()

  def get(path: String): RDD[PortableDataStream] = {
    sivaFilesRDD.get(path) match {
      case None =>
        val rdd = SivaRDDProvider.generateRDD(sc, path)
        sivaFilesRDD.put(path, rdd)
        rdd
      case Some(rdd) => rdd
    }
  }
}

object SivaRDDProvider {
  var provider: SivaRDDProvider = _

  def apply(sc: SparkContext): SivaRDDProvider = {
    if (provider == null) {
      provider = new SivaRDDProvider(sc)
    }

    provider
  }

  private def generateRDD(sc: SparkContext, path: String): RDD[PortableDataStream] =
    sc.binaryFiles(path).map(_._2)
}
