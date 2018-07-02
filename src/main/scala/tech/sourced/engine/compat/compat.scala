package tech.sourced.engine.compat

import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.datasources.{
  LogicalRelation => SparkLogicalRelation
}
import org.apache.spark.sql.sources.BaseRelation

import scala.reflect.runtime.{universe => ru}

private[compat] object Compat {

  def apply[T](s22: T, s23: T): T = SPARK_VERSION match {
    case s if s.startsWith("2.2.") => s22
    case s if s.startsWith("2.3.") => s23
    case _ =>
      throw new RuntimeException(s"Unsupported SPARK_VERSION: $SPARK_VERSION")
  }

  lazy val ClassMirror = ru.runtimeMirror(Compat.getClass.getClassLoader)

}

private[engine] object LogicalRelation {

  def apply(rel: BaseRelation,
            out: Seq[AttributeReference],
            catalog: Option[CatalogTable]): SparkLogicalRelation =
    applyImpl(rel, out, catalog)

  private lazy val applyImpl =
    Compat(applySpark22(_, _, _), applySpark23(_, _, _))

  private lazy val typ = ru.typeOf[SparkLogicalRelation]
  private lazy val classSymbol =
    Compat.ClassMirror.reflectClass(typ.typeSymbol.asClass)
  private lazy val ctor =
    classSymbol.reflectConstructor(typ.decl(ru.termNames.CONSTRUCTOR).asMethod)

  def applySpark22(rel: BaseRelation,
                   out: Seq[AttributeReference],
                   catalog: Option[CatalogTable]): SparkLogicalRelation =
    ctor(rel, out, catalog).asInstanceOf[SparkLogicalRelation]

  def applySpark23(rel: BaseRelation,
                   out: Seq[AttributeReference],
                   catalog: Option[CatalogTable]): SparkLogicalRelation =
    ctor(rel, out, catalog, false).asInstanceOf[SparkLogicalRelation]

  def unapply(arg: SparkLogicalRelation)
    : Option[(BaseRelation, Seq[AttributeReference], Option[CatalogTable])] =
    unapplyImpl(arg)

  private lazy val unapplyImpl = Compat(unapplySpark22(_), unapplySpark23(_))

  def unapplySpark22(arg: SparkLogicalRelation)
    : Option[(BaseRelation, Seq[AttributeReference], Option[CatalogTable])] =
    Some((arg.relation, arg.output, arg.catalogTable))

  def unapplySpark23(arg: SparkLogicalRelation)
    : Option[(BaseRelation, Seq[AttributeReference], Option[CatalogTable])] = {
    val isStreaming = Compat.ClassMirror
      .reflect(arg)
      .reflectField(typ.decl(ru.TermName("isStreaming")).asTerm)
      .get
      .asInstanceOf[Boolean]
    if (isStreaming) {
      None
    } else {
      Some((arg.relation, arg.output, arg.catalogTable))
    }
  }

}
