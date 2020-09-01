package com.emarsys.rdb.connector.mysql

import com.emarsys.rdb.connector.common.defaults.{DefaultSqlWriters, SqlWriter}
import com.emarsys.rdb.connector.common.models.SimpleSelect._

object MySqlWriters extends DefaultSqlWriters {
  implicit override lazy val tableNameWriter: SqlWriter[TableName] = SqlWriter.createTableNameWriter("`", "\\")
  implicit override lazy val fieldNameWriter: SqlWriter[FieldName] = SqlWriter.createFieldNameWriter("`", "\\")
}
