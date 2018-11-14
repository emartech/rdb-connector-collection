package com.emarsys.rdb.connector.bigquery

import cats.Traverse
import cats.data.EitherT
import cats.implicits._
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.TableSchemaDescriptors.{FieldModel, FullTableModel, TableModel}

trait BigQueryMetadata {
  self: BigQueryConnector =>

  override def listTables(): ConnectorResponse[Seq[TableModel]] = {
    bigQueryClient
      .listTables()
      .recover(eitherErrorHandler)
  }

  override def listFields(tableName: String): ConnectorResponse[Seq[FieldModel]] = {
    bigQueryClient
      .listFields(tableName)
      .recover(eitherErrorHandler)
  }

  override def listTablesWithFields(): ConnectorResponse[Seq[FullTableModel]] = {
    val tablesWithFields = for {
      tables <- EitherT(listTables())
      map    <- mapTablesWithFields(tables.toVector)
    } yield makeTablesWithFields(tables, map.toMap)

    tablesWithFields.value.recover(eitherErrorHandler)
  }

  private def mapTablesWithFields[F[_]: Traverse](tables: F[TableModel]) = {
    def getFieldsForTable(table: TableModel) =
      EitherT(listFields(table.name)).map(fieldModel => (table.name, fieldModel))

    tables.traverse(getFieldsForTable)
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
}
