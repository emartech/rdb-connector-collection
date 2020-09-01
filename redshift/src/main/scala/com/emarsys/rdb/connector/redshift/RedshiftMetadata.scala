package com.emarsys.rdb.connector.redshift

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName}
import com.emarsys.rdb.connector.common.models.TableSchemaDescriptors.{FieldModel, FullTableModel, TableModel}
import slick.jdbc.PostgresProfile.api._

import scala.concurrent.Future

trait RedshiftMetadata {
  self: RedshiftConnector =>

  override def listTables(): ConnectorResponse[Seq[TableModel]] = {
    db.run(
        sql"SELECT DISTINCT table_name, table_type  FROM SVV_TABLES WHERE table_schema = $schemaName;"
          .as[(String, String)]
      )
      .map(_.map(parseToTableModel))
      .map(Right(_))
      .recover(eitherErrorHandler)
  }

  override def listFields(tableName: String): ConnectorResponse[Seq[FieldModel]] = {
    db.run(
        sql"SELECT column_name, data_type FROM SVV_COLUMNS WHERE table_name = $tableName AND table_schema = $schemaName;"
          .as[(String, String)]
      )
      .map(_.map(parseToFieldModel))
      .map(fields => {
        if (fields.isEmpty) {
          Left(
            DatabaseError(
              ErrorCategory.FatalQueryExecution,
              ErrorName.TableNotFound,
              s"Table not found: $tableName",
              None,
              None
            )
          )
        } else {
          Right(fields)
        }
      })
      .recover(eitherErrorHandler)
  }

  override def listTablesWithFields(): ConnectorResponse[Seq[FullTableModel]] = {
    val futureMap = listAllFields()
    (for {
      tablesE <- listTables()
      map     <- futureMap
    } yield tablesE.map(makeTablesWithFields(_, map)))
      .recover(eitherErrorHandler)
  }

  private def listAllFields(): Future[Map[String, Seq[FieldModel]]] = {
    db.run(
        sql"SELECT table_name, column_name, data_type FROM SVV_COLUMNS WHERE table_schema = $schemaName;"
          .as[(String, String, String)]
      )
      .map(_.groupBy(_._1).map {
        case (table, b) => table -> b.map { case (_, column, dataType) => parseToFieldModel(column -> dataType) }
      })
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

  private def parseToTableModel(t: (String, String)): TableModel = {
    TableModel(t._1, isTableTypeView(t._2))
  }

  private def isTableTypeView(tableType: String): Boolean = tableType match {
    case "VIEW" => true
    case _      => false
  }

}
