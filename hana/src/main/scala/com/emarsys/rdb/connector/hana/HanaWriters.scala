package com.emarsys.rdb.connector.hana

import com.emarsys.rdb.connector.common.defaults.SqlWriter.{
  createFieldNameWriter,
  createTableNameWriter,
  createValueWriter
}
import com.emarsys.rdb.connector.common.defaults.{DefaultSqlWriters, SqlWriter}
import com.emarsys.rdb.connector.common.models.SimpleSelect._

object HanaWriters extends DefaultSqlWriters {
  implicit override lazy val tableNameWriter: SqlWriter[TableName] = createTableNameWriter("\"", "\\")
  implicit override lazy val fieldNameWriter: SqlWriter[FieldName] = createFieldNameWriter("\"", "\\")
  implicit override lazy val valueWriter: SqlWriter[Value]         = createValueWriter("'", "'")
}
