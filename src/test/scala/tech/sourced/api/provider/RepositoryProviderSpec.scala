package tech.sourced.api.provider

import org.eclipse.jgit.lib.ObjectId
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import tech.sourced.api.{BaseSivaSpec, BaseSparkSpec}

import scala.collection.JavaConverters._

class RepositoryProviderSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  class RepositoryProviderSpec extends FlatSpec with Matchers with BaseSivaSpec with BaseSparkSpec {
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

    "RepositoryProvider" should "fail with different local paths" in {
      val _ = RepositoryProvider("/tmp")

      val ex: RuntimeException = intercept[RuntimeException] {
        RepositoryProvider("/tmp/two")
      }

      ex.getMessage should be("actual provider instance is not intended to be used " +
        "with the localPath provided: /tmp/two")
    }
  }

}
