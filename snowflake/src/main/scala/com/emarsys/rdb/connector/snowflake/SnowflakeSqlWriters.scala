package com.emarsys.rdb.connector.snowflake

import com.emarsys.rdb.connector.common.defaults.{DefaultSqlWriters, SqlWriter}
import com.emarsys.rdb.connector.common.models.SimpleSelect._

object SnowflakeSqlWriters extends DefaultSqlWriters {
  implicit override lazy val tableNameWriter: SqlWriter[TableName] = SqlWriter.createTableNameWriter("`", "\\")
}
