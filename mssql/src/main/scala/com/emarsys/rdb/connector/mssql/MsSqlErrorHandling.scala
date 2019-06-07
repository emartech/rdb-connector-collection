package com.emarsys.rdb.connector.mssql

import java.sql.SQLException

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.defaults.ErrorConverter
import com.emarsys.rdb.connector.common.models.Errors._
import com.microsoft.sqlserver.jdbc.SQLServerException

trait MsSqlErrorHandling {
  import ErrorConverter._

  val MSSQL_STATE_QUERY_CANCELLED            = "HY008"
  val MSSQL_STATE_SYNTAX_ERROR               = "S0001"
  val MSSQL_STATE_SHOWPLAN_PERMISSION_DENIED = "S0004"
  val MSSQL_STATE_PERMISSION_DENIED          = "S0005"
  val MSSQL_STATE_INVALID_OBJECT_NAME        = "S0002"

  val MSSQL_BAD_HOST_ERROR = "08S01"

  val MSSQL_EXPLAIN_PERMISSION_DENIED = "lacking privileges"

  val connectionErrors = Seq(
    MSSQL_BAD_HOST_ERROR
  )

  protected def errorHandler(): PartialFunction[Throwable, ConnectorError] = {
    case ex: SQLServerException if ex.getSQLState == MSSQL_STATE_QUERY_CANCELLED     => QueryTimeout(getErrorMessage(ex))
    case ex: SQLServerException if ex.getSQLState == MSSQL_STATE_SYNTAX_ERROR        => SqlSyntaxError(getErrorMessage(ex))
    case ex: SQLServerException if ex.getSQLState == MSSQL_STATE_PERMISSION_DENIED   => AccessDeniedError(getErrorMessage(ex))
    case ex: SQLServerException if ex.getSQLState == MSSQL_STATE_INVALID_OBJECT_NAME => TableNotFound(getErrorMessage(ex))
    case ex: SQLServerException if ex.getSQLState == MSSQL_STATE_SHOWPLAN_PERMISSION_DENIED =>
      AccessDeniedError(getErrorMessage(ex))
    case ex: SQLException if ex.getMessage.contains(MSSQL_EXPLAIN_PERMISSION_DENIED) => AccessDeniedError(getErrorMessage(ex))
    case ex: SQLException if connectionErrors.contains(ex.getSQLState)               => ConnectionError(ex)
  }

  protected def eitherErrorHandler[T](): PartialFunction[Throwable, Either[ConnectorError, T]] =
    (errorHandler orElse ErrorConverter.default) andThen Left.apply

  protected def streamErrorHandler[A]: PartialFunction[Throwable, Source[A, NotUsed]] =
    (errorHandler orElse ErrorConverter.default) andThen Source.failed

}
