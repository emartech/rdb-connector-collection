package com.emarsys.rdb.connector.common.models

import com.emarsys.rdb.connector.common.models.SimpleSelect.{Fields, TableName, WhereCondition}

case class SimpleSelect(
    fields: Fields,
    table: TableName,
    where: Option[WhereCondition] = None,
    limit: Option[Int] = None,
    distinct: Option[Boolean] = None
)

object SimpleSelect {

  sealed trait Fields

  sealed trait WhereCondition

  case class TableName(t: String)

  case class FieldName(f: String)

  case class Value(v: String)

  case object AllField extends Fields

  case class SpecificFields(fields: Seq[FieldName]) extends Fields

  object SpecificFields {
    def apply(field: FieldName, otherFields: FieldName*): SpecificFields = SpecificFields(field +: otherFields)
    def apply(field: String, otherFields: String*): SpecificFields =
      SpecificFields((field +: otherFields.toList).map(FieldName))
  }

  case class And(conditions: Seq[WhereCondition]) extends WhereCondition

  object And {
    def apply(condition: WhereCondition, otherConditions: WhereCondition*): And =
      And((condition +: otherConditions).toList)
  }

  case class Or(conditions: Seq[WhereCondition]) extends WhereCondition

  object Or {
    def apply(condition: WhereCondition, otherConditions: WhereCondition*): Or =
      Or((condition +: otherConditions).toList)
  }

  case class EqualToValue(field: FieldName, value: Value) extends WhereCondition

  object EqualToValue {
    def apply(field: String, value: String): EqualToValue = EqualToValue(FieldName(field), Value(value))
    def tupled(fv: (FieldName, Value))                    = EqualToValue.apply(fv._1, fv._2)
  }

  case class NotNull(field: FieldName) extends WhereCondition

  case class IsNull(field: FieldName) extends WhereCondition

}
