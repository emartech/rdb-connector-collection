package com.emarsys.rdb.connector.redshift

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName}

trait RedshiftIsOptimized {
  self: RedshiftConnector with RedshiftMetadata =>

  override def isOptimized(table: String, fields: Seq[String]): ConnectorResponse[Boolean] = {
    listTables()
      .map(
        _.map(_.exists(_.name == table))
          .flatMap(
            if (_) Right(true)
            else
              Left(
                DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.TableNotFound, s"Table not found: $table")
              )
          )
      )
      .recover(eitherErrorHandler)
  }
}
