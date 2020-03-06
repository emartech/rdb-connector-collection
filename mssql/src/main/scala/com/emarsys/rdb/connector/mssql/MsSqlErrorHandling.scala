package com.emarsys.rdb.connector.mssql

import java.sql.SQLException

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.defaults.ErrorConverter.default
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName}
import com.microsoft.sqlserver.jdbc.SQLServerException

trait MsSqlErrorHandling {

  private val MSSQL_STATE_QUERY_CANCELLED            = "HY008"
  private val MSSQL_STATE_SYNTAX_ERROR               = "S0001"
  private val MSSQL_DUPLICATE_PRIMARY_KEY            = "23000"
  private val MSSQL_STATE_INVALID_OBJECT_NAME        = "S0002"
  private val MSSQL_STATE_PERMISSION_DENIED          = "S0005"
  private val MSSQL_STATE_SHOWPLAN_PERMISSION_DENIED = "S0004"

  private val MSSQL_INVALID_USER_OR_PASSWORD  = "Login failed for user"
  private val MSSQL_EXPLAIN_PERMISSION_DENIED = "lacking privileges"

  private def errorHandler(): PartialFunction[Throwable, DatabaseError] = {
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
    case ex: SQLException if isInvalidUserOrPassword(ex) =>
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.AccessDeniedError, ex)
    case ex: SQLException if ex.getMessage.contains(MSSQL_EXPLAIN_PERMISSION_DENIED) =>
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.AccessDeniedError, ex)
  }

  private def isInvalidUserOrPassword(sqlException: SQLException): Boolean = {
    Option(sqlException.getCause)
      .flatMap(cause => Option(cause.getMessage))
      .exists(_.contains(MSSQL_INVALID_USER_OR_PASSWORD))
  }

  protected def onDuplicateKey[T](default: => T): PartialFunction[Throwable, T] = {
    case ex: SQLServerException if ex.getSQLState == MSSQL_DUPLICATE_PRIMARY_KEY => default
  }

  protected def eitherErrorHandler[T](): PartialFunction[Throwable, Either[DatabaseError, T]] =
    (errorHandler orElse default) andThen Left.apply

  protected def streamErrorHandler[A]: PartialFunction[Throwable, Source[A, NotUsed]] =
    (errorHandler orElse default) andThen Source.failed
}
