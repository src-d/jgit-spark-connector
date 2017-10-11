package org.apache.spark

import org.apache.spark.util.Utils

object UtilsWrapper {
  def getLocalDir(conf: SparkConf): String = Utils.getLocalDir(conf)
}
