package com.emarsys.rdb.connector.snowflake

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName}

trait SnowflakeIsOptimized {
  self: SnowflakeConnector with SnowflakeMetadata =>
  import cats.syntax.flatMap._

  override def isOptimized(table: String, fields: Seq[String]): ConnectorResponse[Boolean] = {
    listTables()
      .map(
        _.map(_.exists(_.name == table))
          .ifM(
            Right(true),
            Left(DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.TableNotFound, s"Table not found: $table"))
          )
      )
      .recover(eitherErrorHandler())
  }
}
