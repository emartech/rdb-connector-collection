package com.emarsys.rdb.connector.mysql

import cats.data.EitherT
import cats.implicits._
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.defaults.SqlWriter._
import com.emarsys.rdb.connector.common.models.SimpleSelect.TableName
import com.emarsys.rdb.connector.common.models.TableSchemaDescriptors.{FieldModel, FullTableModel, TableModel}
import com.emarsys.rdb.connector.mysql.MySqlWriters._
import slick.jdbc.MySQLProfile.api._

trait MySqlMetadata {
  self: MySqlConnector =>

  override def listTables(): ConnectorResponse[Seq[TableModel]] = {
    db.run(sql"SHOW FULL TABLES".as[(String, String)])
      .map(_.map(parseToTableModel))
      .map(Right(_))
      .recover(eitherErrorHandler())
  }

  override def listFields(tableName: String): ConnectorResponse[Seq[FieldModel]] = {
    db.run(sql"DESC #${TableName(tableName).toSql}".as[(String, String, String, String, String, String)])
      .map(_.map(parseToFieldModel))
      .map(Right(_))
      .recover(handleNotExistingTable(tableName))
      .recover(eitherErrorHandler())
  }

  override def listTablesWithFields(): ConnectorResponse[Seq[FullTableModel]] = {
    (for {
      tables        <- EitherT(listTables())
      fieldsByTable <- EitherT(listAllFieldsByTable())
    } yield makeTablesWithFields(tables, fieldsByTable)).value
  }

  private def listAllFieldsByTable(): ConnectorResponse[Map[String, Seq[FieldModel]]] = {
    db.run(
        sql"select TABLE_NAME, COLUMN_NAME, COLUMN_TYPE from information_schema.columns where table_schema = DATABASE() ORDER BY ORDINAL_POSITION;"
          .as[(String, String, String)]
      )
      .map(_.groupBy(_._1).map { case (a, b) => a -> b.map(x => parseToFieldModel(x._2 -> x._3)) })
      .map(Right(_))
      .recover(eitherErrorHandler())
  }

  private def makeTablesWithFields(
      tableList: Seq[TableModel],
      tableFieldMap: Map[String, Seq[FieldModel]]
  ): Seq[FullTableModel] = {
    tableList
      .map(table => (table, tableFieldMap.get(table.name)))
      .collect {
        case (table, Some(fields)) => FullTableModel(table.name, table.isView, fields)
      }
  }

  private def parseToFieldModel(f: (String, String)): FieldModel = {
    FieldModel(f._1, f._2)
  }

  private def parseToFieldModel(f: (String, String, String, String, String, String)): FieldModel = {
    FieldModel(f._1, f._2)
  }

  private def parseToTableModel(t: (String, String)): TableModel = {
    TableModel(t._1, isTableTypeView(t._2))
  }

  private def isTableTypeView(tableType: String): Boolean = tableType match {
    case "VIEW" => true
    case _      => false
  }
}
