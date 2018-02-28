package com.emarsys.rdb.connector.bigquery

import cats.data.EitherT
import cats.implicits._
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors.ConnectorError
import com.emarsys.rdb.connector.common.models.TableSchemaDescriptors.{FieldModel, FullTableModel, TableModel}

import scala.concurrent.Future

trait BigQueryMetadata {
  self: BigQueryConnector =>

  override def listTables(): ConnectorResponse[Seq[TableModel]] = {
    bigQueryClient.listTables()
  }

  override def listFields(tableName: String): ConnectorResponse[Seq[FieldModel]] = {
    bigQueryClient.listFields(tableName)
  }

  override def listTablesWithFields(): ConnectorResponse[Seq[FullTableModel]] = {
    val tablesWithFields = for {
      tables <- EitherT(listTables())
      map    <- mapTablesWithFields(tables)
    } yield makeTablesWithFields(tables, map.toMap)

    tablesWithFields.value
  }

  private def mapTablesWithFields(tables: Seq[TableModel]) = {
    type ET[A] = EitherT[Future, ConnectorError, A]
    tables
      .map(table => EitherT(listFields(table.name)).map(fieldModel => (table.name, fieldModel)))
      .toList
      .sequence[ET, (String, Seq[FieldModel])]
  }

  private def makeTablesWithFields(tableList: Seq[TableModel],
                                   tableFieldMap: Map[String, Seq[FieldModel]]): Seq[FullTableModel] = {
    tableList
      .map(table => (table, tableFieldMap.get(table.name)))
      .collect {
        case (table, Some(fields)) => FullTableModel(table.name, table.isView, fields)
      }
  }
}
