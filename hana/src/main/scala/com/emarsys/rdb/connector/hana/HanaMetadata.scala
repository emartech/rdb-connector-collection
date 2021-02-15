package com.emarsys.rdb.connector.hana

import cats.data.EitherT
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.defaults.SqlWriter._
import com.emarsys.rdb.connector.common.models.SimpleSelect.TableName
import com.emarsys.rdb.connector.common.models.TableSchemaDescriptors.{FieldModel, FullTableModel, TableModel}
import HanaProfile.api._
import HanaWriters._
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName}
import slick.dbio.DBIOAction
import slick.jdbc.GetResult

trait HanaMetadata {
  self: HanaConnector =>

  implicit val tableModelGetResult: GetResult[TableModel] = GetResult { r =>
    TableModel(r.<<, r.<<)
  }
  implicit val fieldModelGetResult: GetResult[FieldModel] = GetResult { r =>
    FieldModel(r.<<, r.<<)
  }

  override def listTables(): ConnectorResponse[Seq[TableModel]] = {
    val getTablesQuery =
      sql"""SELECT TABLE_NAME, false as "IS_VIEW" from public.tables WHERE SCHEMA_NAME in (SELECT CURRENT_SCHEMA FROM DUMMY)"""
        .as[TableModel]
    val getViewsQuery =
      sql"""select VIEW_NAME, true as "IS_VIEW" from public.views where SCHEMA_NAME in (select CURRENT_SCHEMA from dummy)"""
        .as[TableModel]

    val query = for {
      tables <- getTablesQuery
      views  <- getViewsQuery
    } yield tables ++ views

    run(query)
  }

  override def listFields(tableName: String): ConnectorResponse[Seq[FieldModel]] = {
    val getTableColumnsQuery = sql"""SELECT
                     |  COLUMN_NAME,
                     |  DATA_TYPE_NAME
                     |FROM public.TABLE_COLUMNS
                     |WHERE (SCHEMA_NAME IN (SELECT CURRENT_SCHEMA FROM DUMMY))
                     |  AND TABLE_NAME = $tableName
                     |ORDER BY POSITION
                     |""".stripMargin.as[FieldModel]

    val getViewColumnsQuery = sql"""SELECT
                     |  COLUMN_NAME,
                     |  DATA_TYPE_NAME
                     |FROM public.VIEW_COLUMNS
                     |WHERE (SCHEMA_NAME IN (SELECT CURRENT_SCHEMA FROM DUMMY))
                     |  AND VIEW_NAME = $tableName
                     |ORDER BY POSITION
                     |""".stripMargin.as[FieldModel]

    val query = for {
      tables <- getTableColumnsQuery
      views  <- if (tables.isEmpty) getViewColumnsQuery else DBIOAction.successful(Vector.empty)
    } yield tables ++ views

    run(query).map(_.flatMap { result =>
      if (result.isEmpty) {
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
        Right(result)
      }
    })
  }

  override def listTablesWithFields(): ConnectorResponse[Seq[FullTableModel]] = {
    val getAllColumnsQuery =
      sql""" SELECT TABLE_NAME as "TABLE_NAME", false as "IS_VIEW", COLUMN_NAME, DATA_TYPE_NAME 
         |   FROM public.TABLE_COLUMNS WHERE (SCHEMA_NAME IN (SELECT CURRENT_SCHEMA FROM DUMMY))
         |   
         | UNION ALL
         | 
         | SELECT VIEW_NAME as "TABLE_NAME", true as "IS_VIEW", COLUMN_NAME, DATA_TYPE_NAME 
         |   FROM public.VIEW_COLUMNS WHERE (SCHEMA_NAME IN (SELECT CURRENT_SCHEMA FROM DUMMY))""".stripMargin
        .as[(String, Boolean, String, String)]

    val query = getAllColumnsQuery.map { results =>
      results
        .groupBy { case (tableName, isView, _, _) => (tableName, isView) }
        .map { case ((tableName, isView), columnData) =>
          val fieldModels = columnData.map(row => FieldModel(row._3, row._4))
          FullTableModel(tableName, isView, fieldModels)
        }
        .toSeq
    }

    run(query)
  }
}
