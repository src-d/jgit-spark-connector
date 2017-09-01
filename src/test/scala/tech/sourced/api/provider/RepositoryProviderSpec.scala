package tech.sourced.api.provider

import org.apache.spark.sql.SparkSession
import org.eclipse.jgit.lib.ObjectId

import scala.collection.JavaConverters._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class RepositoryProviderSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  var ss: SparkSession = _
  val resourcePath: String = getClass.getResource("/siva-files").toString

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    ss = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
  }

  "SivaRDDProvider" should "return always the same instance" in {
    val prov = SivaRDDProvider(ss.sparkContext)
    val prov2 = SivaRDDProvider(ss.sparkContext)

    prov should equal(prov2)
    prov should not equal new SivaRDDProvider(ss.sparkContext)

  }

  "SivaRDDProvider" should "return the exact name of siva files" in {
    val prov = SivaRDDProvider(ss.sparkContext)

    val sivaRDD = prov.get(resourcePath)

    sivaRDD.count() should be(3)
  }

  "RepositoryProvider" should "read correctly siva repositories" in {
    val prov = SivaRDDProvider(ss.sparkContext)

    val sivaRDD = prov.get(resourcePath)

    val refs = sivaRDD.flatMap(pds => {
      val repo = RepositoryProvider("/tmp").get(pds)

      repo.getAllRefs.asScala.mapValues(r => ObjectId.toString(r.getPeeledObjectId))
    }).collect()

    refs.length should be(56)
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    ss = null
  }
}
