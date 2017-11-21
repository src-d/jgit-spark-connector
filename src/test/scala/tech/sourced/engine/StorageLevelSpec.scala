package tech.sourced.engine

import org.scalatest.{FlatSpec, Matchers}

class StorageLevelSpec  extends FlatSpec with Matchers with BaseSivaSpec with BaseSparkSpec {

  var engine: Engine = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    engine = Engine(ss, resourcePath)
  }

  "A Dataframe" should "work with all storage levels" in {
    import org.apache.spark.storage.StorageLevel._
    val storageLevels = List(
      DISK_ONLY,
      DISK_ONLY_2,
      MEMORY_AND_DISK,
      MEMORY_AND_DISK_2,
      MEMORY_AND_DISK_SER,
      MEMORY_AND_DISK_SER_2,
      MEMORY_ONLY,
      MEMORY_ONLY_2,
      MEMORY_ONLY_SER,
      MEMORY_ONLY_SER_2,
      NONE,
      OFF_HEAP
    )

    storageLevels.foreach(level => {
      val df = engine.getRepositories.persist(level)
      df.count()
      df.unpersist()
    })
  }
}
