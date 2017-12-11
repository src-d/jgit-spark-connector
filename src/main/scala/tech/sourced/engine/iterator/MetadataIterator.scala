package tech.sourced.engine.iterator

import java.sql.{Connection, DriverManager, ResultSet}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.types.{ArrayType, BooleanType, TimestampType}

class MetadataIterator(fields: Seq[Attribute],
                       dbPath: String,
                       sql: String)
  extends Iterator[Map[String, Any]] with Logging {

  private val iter = new JDBCQueryIterator(fields, dbPath, sql)

  override def hasNext: Boolean = iter.hasNext

  override def next(): Map[String, Any] = {
    val values = iter.next()
    Map[String, Any](fields.zipWithIndex.map {
      case (attr, idx) if attr.dataType == BooleanType =>
        (attr.name, values(idx) match {
          case 0 => false
          case 1 => true
          case _ => null
        })
      case (attr, idx) if attr.dataType.isInstanceOf[ArrayType] =>
        (attr.name, values(idx).toString.split("\\|"))
      case (attr, idx) if attr.dataType == TimestampType =>
        (attr.name, new java.sql.Timestamp(values(idx).asInstanceOf[Long]))
      case (attr, idx) =>
        (attr.name, values(idx))
    }: _*)
  }

  def close(): Unit = iter.close()

}

class JDBCQueryIterator(fields: Seq[Attribute],
                        dbPath: String,
                        sql: String)
  extends Iterator[Array[Any]] with Logging {

  private var rs: ResultSet = _
  private var conn: Connection = _
  private var nextCollected = false
  private var hasRows = false

  private[iterator] def close(): Unit = {
    try {
      if (rs != null && !rs.isClosed) {
        rs.close()
      }
    } finally {
      if (conn != null && !conn.isClosed) {
        try {
          conn.close()
        } catch {
          case e: Exception => log.warn(s"could not close connection", e)
        }
      }
    }
  }

  override def hasNext: Boolean = {
    if (rs == null) {
      initializeResultSet()
    } else if (hasRows && !nextCollected) {
      // FIXME: RDD groupBy somehow calls hasNext twice, so we can't
      // advance the cursor until the next row has been collected to make sure
      // we don't skip rows.
      return true
    }

    try {
      if (!rs.isClosed && rs.next) {
        hasRows = true
        nextCollected = false
        true
      } else {
        close()
        false
      }
    } catch {
      case e: Exception =>
        log.warn(s"caught an exception in JDBCIterator.hasNext", e)
        close()
        false
    }
  }

  private def initializeResultSet(): Unit = {
    conn = DriverManager.getConnection(s"jdbc:sqlite:$dbPath")
    val stmt = conn.prepareStatement(sql)
    try {
      rs = stmt.executeQuery()
    } catch {
      case e: Exception =>
        log.warn(s"could not execute query", e)
        close()
    }
  }

  override def next(): Array[Any] = {
    nextCollected = true
    fields.zipWithIndex
      .map(f => rs.getObject(f._2 + 1))
      .toArray
      .asInstanceOf[Array[Any]]
  }

}
