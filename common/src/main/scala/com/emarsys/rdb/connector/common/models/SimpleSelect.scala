package com.emarsys.rdb.connector.common.models

import com.emarsys.rdb.connector.common.models.SimpleSelect.{Fields, TableName, WhereCondition}

case class SimpleSelect(fields: Fields,
                        table: TableName,
                        where: Option[WhereCondition] = None,
                        limit: Option[Int] = None,
                        distinct: Option[Boolean] = None)

object SimpleSelect {

  sealed trait Fields

  sealed trait WhereCondition

  case class TableName(t: String)

  case class FieldName(f: String)

  case class Value(v: String)

  case object AllField extends Fields

  case class SpecificFields(fields: Seq[FieldName]) extends Fields

  case class And(conditions: Seq[WhereCondition]) extends WhereCondition

  case class Or(conditions: Seq[WhereCondition]) extends WhereCondition

  case class EqualToValue(field: FieldName, value: Value) extends WhereCondition

  case class NotNull(field: FieldName) extends WhereCondition

  case class IsNull(field: FieldName) extends WhereCondition

}
