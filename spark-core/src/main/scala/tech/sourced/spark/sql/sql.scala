package tech.sourced.spark

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession}
import org.apache.spark.sql.functions.udf
import tech.sourced.api._

package object sql {

  implicit val uuidEncoder: Encoder[UUID] = Encoders.kryo[UUID]
  implicit val repositoryDescriptorEncoder: Encoder[RepositoryDescriptor] = Encoders.kryo[RepositoryDescriptor]
  implicit val signatureEncoder: Encoder[Signature] = Encoders.kryo[Signature]
  implicit val commitEncoder: Encoder[Commit] = Encoders.kryo[Commit]

  def createDataFrame(ss: SparkSession, repositories: GitRepositories): DataFrame = {
    ss.createDataFrame(loadAllFiles(repositories))
  }

  object udfs {
    val detectLanguageByExtension: UserDefinedFunction = udf(EnryLanguageDetector.detect(_: String))
    val detectLanguage: UserDefinedFunction = udf(EnryLanguageDetector.detect(_: String, _: Array[Byte]))
    val uast: UserDefinedFunction = udf(UAST.parse(_: String, _: Array[Byte]))
  }

  def setupSparkSession(ss: SparkSession): Unit = {
    ss.udf.register("detect_language", EnryLanguageDetector.detect(_: String))
    ss.udf.register("detect_language", EnryLanguageDetector.detect(_: String, _: Array[Byte]))
    ss.udf.register("uast", UAST.parse(_: String, _: Array[Byte]))
  }

}
