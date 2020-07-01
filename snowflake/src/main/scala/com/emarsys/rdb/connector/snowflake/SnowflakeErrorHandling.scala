package com.emarsys.rdb.connector.snowflake

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.defaults.ErrorConverter
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName}
import net.snowflake.client.jdbc.SnowflakeSQLException
import net.snowflake.client.jdbc.internal.snowflake.common.core.SqlState

trait SnowflakeErrorHandling {
  import ErrorConverter._

  private val errorHandler: PartialFunction[Throwable, DatabaseError] = {
    case e: SnowflakeSQLException if e.getSQLState == SqlState.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION =>
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SqlSyntaxError, e)
    case e: SnowflakeSQLException if e.getSQLState == SqlState.BASE_TABLE_OR_VIEW_NOT_FOUND =>
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.TableNotFound, e)
    case e: SnowflakeSQLException if e.getSQLState == SqlState.QUERY_CANCELED =>
      DatabaseError(ErrorCategory.Timeout, ErrorName.QueryTimeout, e)
    case e: SnowflakeSQLException if e.getSQLState == SqlState.SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION =>
      if (e.getMessage.contains("username") || e.getMessage.contains("password") || e.getMessage.contains("DATABASE") || e.getMessage.contains("SCHEMA")) {
        DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.ConnectionConfigError, e)
      } else {
        // TODO: we should monitor this case, it is currently unknown when this can happen
        DatabaseError(ErrorCategory.Transient, ErrorName.TransientDbError, e)
      }
  }

  protected def eitherErrorHandler[T](): PartialFunction[Throwable, Either[DatabaseError, T]] =
    errorHandler.orElse(default).andThen(Left.apply(_))

  protected def streamErrorHandler[A]: PartialFunction[Throwable, Source[A, NotUsed]] =
    errorHandler.orElse(default).andThen(Source.failed(_))
}
