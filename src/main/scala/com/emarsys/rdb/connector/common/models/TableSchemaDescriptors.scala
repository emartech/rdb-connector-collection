package com.emarsys.rdb.connector.common.models

object TableSchemaDescriptors {
  case class TableModel(name: String, isView: Boolean)
  case class FieldModel(name: String, columnType: String)
}
