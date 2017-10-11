package tech.sourced.api.iterator

import org.apache.spark.UtilsWrapper
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.eclipse.jgit.lib.Repository
import org.scalatest.{Matchers, Suite}
import tech.sourced.api.provider.{RepositoryProvider, SivaRDDProvider}
import tech.sourced.api.{BaseSivaSpec, BaseSparkSpec}

trait BaseRootedRepoIterator extends Suite with BaseSparkSpec with BaseSivaSpec with Matchers {
  lazy val prov: SivaRDDProvider = SivaRDDProvider(ss.sparkContext)
  lazy val rdd: RDD[PortableDataStream] = prov.get(resourcePath)

  lazy val pds: PortableDataStream = rdd.filter(pds => pds.getPath()
    .contains("fff7062de8474d10a67d417ccea87ba6f58ca81d.siva")).first()
  lazy val repo: Repository = RepositoryProvider("/tmp").get(pds)

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
