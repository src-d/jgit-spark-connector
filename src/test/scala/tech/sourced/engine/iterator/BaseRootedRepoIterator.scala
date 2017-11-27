package tech.sourced.engine.iterator

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.eclipse.jgit.lib.Repository
import org.scalatest.{Matchers, Suite}
import tech.sourced.engine.provider.{RepositoryProvider, RepositorySource, RepositoryRDDProvider}
import tech.sourced.engine.{BaseSivaSpec, BaseSparkSpec}

trait BaseRootedRepoIterator extends Suite with BaseSparkSpec with BaseSivaSpec with Matchers {
  lazy val prov: RepositoryRDDProvider = RepositoryRDDProvider(ss.sparkContext)
  lazy val rdd: RDD[RepositorySource] = prov.get(resourcePath)

  lazy val source: RepositorySource = rdd.filter(source => source.pds.getPath()
    .contains("fff7062de8474d10a67d417ccea87ba6f58ca81d.siva")).first()
  lazy val repo: Repository = RepositoryProvider("/tmp").get(source)

  def testIterator(iterator: (Repository) => Iterator[Row],
                   matcher: (Int, Row) => Unit,
                   total: Int,
                   columnsCount: Int): Unit = {
    val ri: Iterator[Row] = iterator(repo)

    var count: Int = 0
    while (ri.hasNext) {
      val row: Row = ri.next()
      row.length should be(columnsCount)
      matcher(count, row)
      count += 1
    }

    count should be(total)
  }

}
