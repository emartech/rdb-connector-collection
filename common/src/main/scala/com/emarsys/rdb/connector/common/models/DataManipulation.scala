package com.emarsys.rdb.connector.common.models

object DataManipulation {
  type Record = Map[String, FieldValueWrapper]

  type Criteria = Record

  case class UpdateDefinition(search: Criteria, update: Record)

  case class StringedUpdateDefinition(search: Map[String, String], update: Map[String, String])

  sealed trait FieldValueWrapper

  object FieldValueWrapper {

    case class StringValue(string: String) extends FieldValueWrapper

    case class IntValue(int: Int) extends FieldValueWrapper

    case class BigDecimalValue(number: BigDecimal) extends FieldValueWrapper

    case class BooleanValue(boolean: Boolean) extends FieldValueWrapper

    case object NullValue extends FieldValueWrapper

  }
}
