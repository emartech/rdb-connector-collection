package com.emarsys.rdb.connector.bigquery

import com.emarsys.rdb.connector.bigquery.BigQueryConnector.BigQueryConnectionConfig
import com.emarsys.rdb.connector.common.defaults.SqlWriter.createValueWriter
import com.emarsys.rdb.connector.common.defaults.{DefaultSqlWriters, SqlWriter}
import com.emarsys.rdb.connector.common.models.SimpleSelect
import com.emarsys.rdb.connector.common.models.SimpleSelect.{FieldName, TableName}
import com.emarsys.rdb.connector.common.models.TableSchemaDescriptors.FieldModel

case class BigQueryWriter(config: BigQueryConnectionConfig, fields: Seq[FieldModel]) extends DefaultSqlWriters {

  override implicit lazy val tableNameWriter: SqlWriter[TableName] =
    (tableName: TableName) => s"${config.dataset}.${tableName.t}"

  override implicit lazy val fieldNameWriter: SqlWriter[SimpleSelect.FieldName] =
    fieldName => fieldName.f

  override implicit lazy val valueWriter: SqlWriter[SimpleSelect.Value] =
    createValueWriter("\"", "\\")

  override implicit lazy val equalToValueWriter: SqlWriter[SimpleSelect.EqualToValue] = {
    equalTo =>
      val fieldType = fields.find( f => f.name == equalTo.field.f).map(_.columnType).getOrElse("STRING")
      val valueAsSql = fieldType match {
        case "INT64" | "FLOAT64" | "INTEGER" | "FLOAT" => equalTo.value.v
        case "BOOL" | "BOOLEAN" if equalTo.value.v == "0" => "FALSE"
        case "BOOL" | "BOOLEAN" if equalTo.value.v == "1" => "TRUE"
        case "BOOL" | "BOOLEAN" => equalTo.value.v.toUpperCase
        case _ => valueWriter.write(equalTo.value)
      }

      s"${equalTo.field.f}=$valueAsSql"
  }

  def simpleSelectWithGroupLimitWriter(references: Seq[String], groupLimit: Int): SqlWriter[SimpleSelect] = (ss: SimpleSelect) => {
    import SqlWriter._
    val rowNumName = "a32ff46896"
    val simpleSelectAsSql = ss.toSql
    val refsAsString = references.map(FieldName).map(_.toSql).mkString(",")
    s"""
       |SELECT * FROM (
       |  SELECT *, row_number() over (partition by $refsAsString) as $rowNumName
       |  FROM ( $simpleSelectAsSql )
       |)
       |WHERE $rowNumName <= $groupLimit
       |""".stripMargin
  }

}
