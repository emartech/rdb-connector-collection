package com.emarsys.rdb.connector.common.defaults

import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper
import com.emarsys.rdb.connector.common.models.SimpleSelect.Value

trait FieldValueConverter[T <: FieldValueWrapper] {
  def convert(fieldValueWrapper: T): Option[Value]
}

trait FieldValueConverters {
  implicit val stringValueConverter: FieldValueConverter[FieldValueWrapper.StringValue]
  implicit val intValueConverter: FieldValueConverter[FieldValueWrapper.IntValue]
  implicit val bigDecimalValueConverter: FieldValueConverter[FieldValueWrapper.BigDecimalValue]
  implicit val booleanValueConverter: FieldValueConverter[FieldValueWrapper.BooleanValue]
  implicit val nullValueConverter: FieldValueConverter[FieldValueWrapper.NullValue.type]

  implicit def fieldValueConverter: FieldValueConverter[FieldValueWrapper] = {
    case value: FieldValueWrapper.StringValue     => stringValueConverter.convert(value)
    case value: FieldValueWrapper.IntValue        => intValueConverter.convert(value)
    case value: FieldValueWrapper.BigDecimalValue => bigDecimalValueConverter.convert(value)
    case value: FieldValueWrapper.BooleanValue    => booleanValueConverter.convert(value)
    case value: FieldValueWrapper.NullValue.type  => nullValueConverter.convert(value)
  }
}

trait DefaultFieldValueConverters extends FieldValueConverters {
  implicit val stringValueConverter: FieldValueConverter[FieldValueWrapper.StringValue] =
    (fieldValueWrapper: FieldValueWrapper.StringValue) => Some(Value(fieldValueWrapper.string))

  implicit val intValueConverter: FieldValueConverter[FieldValueWrapper.IntValue] =
    (fieldValueWrapper: FieldValueWrapper.IntValue) => Some(Value(fieldValueWrapper.int.toString))

  implicit val bigDecimalValueConverter: FieldValueConverter[FieldValueWrapper.BigDecimalValue] =
    (fieldValueWrapper: FieldValueWrapper.BigDecimalValue) => Some(Value(fieldValueWrapper.number.toString()))

  implicit val booleanValueConverter: FieldValueConverter[FieldValueWrapper.BooleanValue] =
    (fieldValueWrapper: FieldValueWrapper.BooleanValue) => Some(Value(fieldValueWrapper.boolean.toString))

  implicit val nullValueConverter: FieldValueConverter[FieldValueWrapper.NullValue.type] =
    (_: FieldValueWrapper.NullValue.type) => None
}

object DefaultFieldValueConverters extends DefaultFieldValueConverters

object FieldValueConverter {

  implicit class FieldValueConverterSyntax[T <: FieldValueWrapper](fieldValue: T) {
    def toSimpleSelectValue(implicit converter: FieldValueConverter[T]): Option[Value] = converter.convert(fieldValue)
  }

}
