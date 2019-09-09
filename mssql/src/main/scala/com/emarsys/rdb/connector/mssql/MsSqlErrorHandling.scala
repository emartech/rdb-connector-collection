package com.emarsys.rdb.connector.mssql

import java.sql.SQLException

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.microsoft.sqlserver.jdbc.SQLServerException
import com.emarsys.rdb.connector.common.defaults.ErrorConverter.default
import com.emarsys.rdb.connector.common.models.Errors.{ConnectorError, DatabaseError, ErrorCategory, ErrorName}

trait MsSqlErrorHandling {

  val MSSQL_STATE_QUERY_CANCELLED            = "HY008"
  val MSSQL_STATE_SYNTAX_ERROR               = "S0001"
  val MSSQL_DUPLICATE_PRIMARY_KEY            = "23000"
  val MSSQL_STATE_INVALID_OBJECT_NAME        = "S0002"
  val MSSQL_STATE_PERMISSION_DENIED          = "S0005"
  val MSSQL_STATE_SHOWPLAN_PERMISSION_DENIED = "S0004"

  val MSSQL_EXPLAIN_PERMISSION_DENIED = "lacking privileges"

  protected def errorHandler(): PartialFunction[Throwable, ConnectorError] = {
    case ex: SQLServerException if ex.getSQLState == MSSQL_STATE_QUERY_CANCELLED =>
      DatabaseError(ErrorCategory.Timeout, ErrorName.QueryTimeout, ex)
    case ex: SQLServerException if ex.getSQLState == MSSQL_STATE_SYNTAX_ERROR =>
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SqlSyntaxError, ex)
    case ex: SQLServerException if ex.getSQLState == MSSQL_DUPLICATE_PRIMARY_KEY =>
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SqlSyntaxError, ex)
    case ex: SQLServerException if ex.getSQLState == MSSQL_STATE_INVALID_OBJECT_NAME =>
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.TableNotFound, ex)
    case ex: SQLServerException if ex.getSQLState == MSSQL_STATE_PERMISSION_DENIED =>
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.AccessDeniedError, ex)
    case ex: SQLServerException if ex.getSQLState == MSSQL_STATE_SHOWPLAN_PERMISSION_DENIED =>
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.AccessDeniedError, ex)
    case ex: SQLException if ex.getMessage.contains(MSSQL_EXPLAIN_PERMISSION_DENIED) =>
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.AccessDeniedError, ex)
  }

  protected def onDuplicateKey[T](default: => T): PartialFunction[Throwable, T] = {
    case ex: SQLServerException if ex.getSQLState == MSSQL_DUPLICATE_PRIMARY_KEY => default
  }

  protected def eitherErrorHandler[T](): PartialFunction[Throwable, Either[ConnectorError, T]] =
    (errorHandler orElse default) andThen Left.apply

  protected def streamErrorHandler[A]: PartialFunction[Throwable, Source[A, NotUsed]] =
    (errorHandler orElse default) andThen Source.failed
}
