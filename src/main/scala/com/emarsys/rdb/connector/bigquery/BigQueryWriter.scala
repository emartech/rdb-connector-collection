package com.emarsys.rdb.connector.bigquery

import com.emarsys.rdb.connector.common.defaults.SqlWriter.createValueWriter
import com.emarsys.rdb.connector.common.defaults.{DefaultSqlWriters, SqlWriter}
import com.emarsys.rdb.connector.common.models.SimpleSelect
import com.emarsys.rdb.connector.common.models.SimpleSelect.TableName

trait BigQueryWriter extends DefaultSqlWriters {
  self: BigQueryConnector =>

  override implicit lazy val tableNameWriter: SqlWriter[TableName] =
    (tableName: TableName) => s"${config.dataset}.${tableName.t}"

  override implicit lazy val fieldNameWriter: SqlWriter[SimpleSelect.FieldName] =
    fieldName => fieldName.f

  override implicit lazy val valueWriter: SqlWriter[SimpleSelect.Value] =
    createValueWriter("\"", "\\")
}
