package tech.sourced.engine

import java.sql.{DriverManager, PreparedStatement}

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types._

private[engine] object TableBuilder {
  def columnSql(field: StructField): String =
    s"`${field.name}` ${sqlType(field.dataType)}" +
      (if (!field.nullable) s" NOT NULL" else "")

  def pkSql(cols: Seq[String]): Option[String] = if (cols.isEmpty) {
    None
  } else {
    Some(s"PRIMARY KEY (${cols.map(v => s"`$v`").mkString(", ")})")
  }

  def indexSql(table: String, col: String): String =
    s"CREATE INDEX `${table}_${col}_idx` ON $table(`$col`)"

  def sqlType(dt: DataType): String = dt match {
    case IntegerType => "INTEGER"
    case LongType | TimestampType => "BIGINT"
    case DoubleType => "DOUBLE PRECISION"
    case FloatType => "REAL"
    case ShortType | ByteType => "TINYINT"
    case StringType => "TEXT"
    case _ => throw new SparkException(s"there is no SQLite type for datatype $dt")
  }
}

private[engine] case class Table(name: String,
                                 pks: Seq[String],
                                 indexes: Seq[String]) extends Logging {
  private def sql(schema: StructType): Seq[String] = {
    Seq(s"CREATE TABLE $name (" +
      (schema.map(TableBuilder.columnSql) ++ TableBuilder.pkSql(pks)).mkString(",\n")
      + s")") ++
      pks.map(TableBuilder.indexSql(name, _))
  }

  def create(dbPath: String, schema: StructType): Unit = {
    val conn = DriverManager.getConnection(s"jdbc:sqlite:$dbPath")
    conn.setAutoCommit(false)
    try {
      sql(schema).foreach(sql => {
        log.debug(s"executing SQL statement for table `$name`: `$sql`")
        var stmt: PreparedStatement = null
        try {
          stmt = conn.prepareStatement(sql)
          stmt.execute()
        } finally {
          if (stmt != null) {
            stmt.close()
          }
        }
      })
      conn.commit()
    } catch {
      case e: Exception =>
        log.error(s"unable to create table $name and its indexes", e)
        conn.rollback()
    } finally {
      conn.close()
    }
  }
}

object Tables {

  val repositories = Table(
    prefix("repositories"),
    Seq("id"),
    Seq("repository_path")
  )

  val references = Table(
    prefix("references"),
    Seq("name", "repository_id"),
    Seq()
  )

  val commits = Table(
    prefix("commits"),
    Seq("hash"),
    Seq()
  )

  val repoHasCommits = Table(
    prefix("repository_has_commits"),
    Seq("hash", "repository_id", "reference_name"),
    Seq("index")
  )

  val treeEntries = Table(
    prefix("tree_entries"),
    // blob id can point to several paths, so we need this overly complex composite pk
    Seq("blob", "path", "commit_hash"),
    Seq()
  )

  def apply(name: String): Table = name match {
    case "repositories" => repositories
    case "references" => references
    case "commits" => commits
    case "repository_has_commits" => repoHasCommits
    case "tree_entries" => treeEntries
  }

  def prefix(name: String): String = s"engine_$name"
}
