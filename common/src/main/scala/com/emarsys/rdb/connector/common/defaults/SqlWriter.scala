package com.emarsys.rdb.connector.common.defaults

import scala.language.implicitConversions
import com.emarsys.rdb.connector.common.models.SimpleSelect
import com.emarsys.rdb.connector.common.models.SimpleSelect._

import scala.annotation.implicitNotFound

@implicitNotFound(msg = "Cannot find implicit SqlWriter type class for ${T}")
trait SqlWriter[T] {
  def write(obj: T): String
}

object SqlWriter {

  implicit class WritableSqlElement[T](any: T) {
    def toSql(implicit writer: SqlWriter[T]): String = writer.write(any)
  }

  implicit def func2LazyWriter[T](f: T => String): SqlWriter[T] = lazyWriter((obj: T) => f(obj))

  def lazyWriter[T](writer: => SqlWriter[T]): SqlWriter[T] = new SqlWriter[T] {
    private lazy val delegate = writer

    def write(x: T): String = delegate.write(x)
  }

  def createTableNameWriter(symbol: String, escape: String): SqlWriter[TableName] =
    (tableName: TableName) => createEscapeQuoter(symbol, escape, tableName.t)

  def createFieldNameWriter(symbol: String, escape: String): SqlWriter[FieldName] =
    (fieldName: FieldName) => createEscapeQuoter(symbol, escape, fieldName.f)

  def createValueWriter(symbol: String, escape: String): SqlWriter[Value] =
    (value: Value) => createEscapeQuoter(symbol, escape, value.v)

  def createAllFieldWriter(symbol: String): SqlWriter[AllField.type] =
    (_: AllField.type) => symbol

  def createSpecificFieldsWriter(symbol: String)(implicit w: SqlWriter[FieldName]): SqlWriter[SpecificFields] =
    (x: SpecificFields) => x.fields.map(_.toSql).mkString(symbol)

  def createIsNullWriter(isNullFormat: String)(implicit w: SqlWriter[FieldName]): SqlWriter[IsNull] =
    (x: IsNull) => isNullFormat.format(x.field.toSql)

  def createNotNullWriter(notNullFormat: String)(implicit w: SqlWriter[FieldName]): SqlWriter[NotNull] =
    (x: NotNull) => notNullFormat.format(x.field.toSql)

  def createEqualToValueWriter(
      symbol: String
  )(implicit w1: SqlWriter[FieldName], w2: SqlWriter[Value]): SqlWriter[EqualToValue] =
    (x: EqualToValue) => s"${x.field.toSql}$symbol${x.value.toSql}"

  def createEscapeQuoter(symbol: String, escape: String, name: String): String = {
    s"$symbol${escaping(symbol, escape, name)}$symbol"
  }

  def conditionWriter(symbol: String, conditions: Seq[WhereCondition])(
      implicit w: SqlWriter[WhereCondition]
  ): String = {
    if (conditions.size == 1) {
      conditions.head.toSql
    } else {
      s"(${conditions.map(_.toSql).mkString(symbol)})"
    }
  }

  private def escaping(symbol: String, escape: String, text: String): String = {
    if (text == null) {
      null
    } else {
      val escapedText = if (escape == "") text else text.replace(escape, escape * 2)
      val symbolEscapedText =
        if (escape == "" || symbol == "") escapedText else escapedText.replace(symbol, s"$escape$symbol")
      symbolEscapedText
    }
  }

}

trait DefaultSqlWriters {

  import SqlWriter._

  implicit lazy val tableNameWriter: SqlWriter[TableName]           = createTableNameWriter("\"", "\\")
  implicit lazy val fieldNameWriter: SqlWriter[FieldName]           = createFieldNameWriter("\"", "\\")
  implicit lazy val valueWriter: SqlWriter[Value]                   = createValueWriter("'", "\\")
  implicit lazy val allFieldWriter: SqlWriter[AllField.type]        = createAllFieldWriter("*")
  implicit lazy val specificFieldsWriter: SqlWriter[SpecificFields] = createSpecificFieldsWriter(",")
  implicit lazy val isNullWriter: SqlWriter[IsNull]                 = createIsNullWriter("%s IS NULL")
  implicit lazy val notNullWriter: SqlWriter[NotNull]               = createNotNullWriter("%s IS NOT NULL")
  implicit lazy val equalToValueWriter: SqlWriter[EqualToValue]     = createEqualToValueWriter("=")
  implicit lazy val orWriter: SqlWriter[Or]                         = (x: Or) => conditionWriter(" OR ", x.conditions)
  implicit lazy val andWriter: SqlWriter[And]                       = (x: And) => conditionWriter(" AND ", x.conditions)

  implicit lazy val whereConditionWriter: SqlWriter[WhereCondition] = {
    case x: EqualToValue => x.toSql
    case x: IsNull       => x.toSql
    case x: NotNull      => x.toSql
    case x: Or           => x.toSql
    case x: And          => x.toSql
  }

  implicit lazy val fieldsWriter: SqlWriter[Fields] = {
    case x: AllField.type  => x.toSql
    case x: SpecificFields => x.toSql
  }

  implicit lazy val directionCriteriaWriter: SqlWriter[Direction] = {
    case Direction.Ascending  => "ASC"
    case Direction.Descending => "DESC"
  }

  implicit lazy val sortCriteriaWriter: SqlWriter[SortCriteria] =
    (obj: SortCriteria) => s"${obj.field.toSql} ${obj.direction.toSql}"

  implicit lazy val sortCriteriaListWriter: SqlWriter[List[SortCriteria]] = (obj: List[SortCriteria]) => {
    val sortCriteriaStrings = obj.map(_.toSql)

    if (sortCriteriaStrings.nonEmpty) {
      sortCriteriaStrings.mkString(" ORDER BY ", ", ", "")
    } else {
      ""
    }
  }

  implicit lazy val simpleSelectWriter: SqlWriter[SimpleSelect] = (ss: SimpleSelect) => {
    val distinct = if (ss.distinct.getOrElse(false)) "DISTINCT " else ""
    val head     = s"SELECT $distinct${ss.fields.toSql} FROM ${ss.table.toSql}"
    val where    = ss.where.map(_.toSql).map(" WHERE " + _).getOrElse("")
    val limit    = ss.limit.map(" LIMIT " + _).getOrElse("")
    val orderBy  = ss.orderBy.toSql

    s"$head$where$orderBy$limit"
  }
}

object DefaultSqlWriters extends DefaultSqlWriters
