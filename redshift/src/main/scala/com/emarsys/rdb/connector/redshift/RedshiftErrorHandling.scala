package com.emarsys.rdb.connector.redshift

import java.sql.SQLException

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.defaults.ErrorConverter
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName}

trait RedshiftErrorHandling {

  val REDSHIFT_STATE_GENERAL_ERROR        = "HY000"
  val REDSHIFT_STATE_QUERY_CANCELLED      = "57014"
  val REDSHIFT_STATE_SYNTAX_ERROR         = "42601"
  val REDSHIFT_STATE_AMBIGUOUS_COLUMN_REF = "42702"
  val REDSHIFT_STATE_PERMISSION_DENIED    = "42501"
  val REDSHIFT_STATE_RELATION_NOT_FOUND   = "42P01"

  val REDSHIFT_MESSAGE_CONNECTION_TIMEOUT = "Connection is not available, request timed out after"
  val REDSHIFT_MESSAGE_TCP_SOCKET_TIMEOUT = "The TCP Socket has timed out while waiting for response"

  val REDSHIFT_STATE_UNABLE_TO_CONNECT       = "08001"
  val REDSHIFT_AUTHORIZATION_NAME_IS_INVALID = "28000"
  val REDSHIFT_SERVER_PROCESS_IS_TERMINATING = "08006"
  val REDSHIFT_INVALID_PASSWORD              = "28P01"

  val connectionErrors = Seq(
    REDSHIFT_STATE_UNABLE_TO_CONNECT,
    REDSHIFT_AUTHORIZATION_NAME_IS_INVALID,
    REDSHIFT_SERVER_PROCESS_IS_TERMINATING,
    REDSHIFT_INVALID_PASSWORD
  )

  private def errorHandler: PartialFunction[Throwable, DatabaseError] = {
    case ex: SQLException if isConnectionTimeout(ex) =>
      DatabaseError(ErrorCategory.Timeout, ErrorName.ConnectionTimeout, ex)
    case ex: SQLException if isTcpSocketTimeout(ex) =>
      DatabaseError(ErrorCategory.Timeout, ErrorName.QueryTimeout, ex)
    case ex: SQLException if ex.getSQLState == REDSHIFT_STATE_QUERY_CANCELLED =>
      DatabaseError(ErrorCategory.Timeout, ErrorName.QueryTimeout, ex)
    case ex: SQLException if ex.getSQLState == REDSHIFT_STATE_SYNTAX_ERROR =>
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SqlSyntaxError, ex)
    case ex: SQLException if ex.getSQLState == REDSHIFT_STATE_AMBIGUOUS_COLUMN_REF =>
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SqlSyntaxError, ex)
    case ex: SQLException if ex.getSQLState == REDSHIFT_STATE_PERMISSION_DENIED =>
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.AccessDeniedError, ex)
    case ex: SQLException if ex.getSQLState == REDSHIFT_STATE_RELATION_NOT_FOUND =>
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.TableNotFound, ex)
    case ex: SQLException if connectionErrors.contains(ex.getSQLState) =>
      DatabaseError(ErrorCategory.Unknown, ErrorName.Unknown, ex)
  }

  private def isConnectionTimeout(ex: SQLException): Boolean = {
    ex.getSQLState == REDSHIFT_STATE_GENERAL_ERROR &&
    ex.getMessage.contains(REDSHIFT_MESSAGE_CONNECTION_TIMEOUT)
  }

  private def isTcpSocketTimeout(ex: SQLException): Boolean = {
    ex.getSQLState == REDSHIFT_STATE_GENERAL_ERROR &&
    ex.getMessage.contains(REDSHIFT_MESSAGE_TCP_SOCKET_TIMEOUT)
  }

  protected def eitherErrorHandler[T]: PartialFunction[Throwable, Either[DatabaseError, T]] =
    errorHandler.orElse(ErrorConverter.default).andThen(Left.apply(_))

  protected def streamErrorHandler[A]: PartialFunction[Throwable, Source[A, NotUsed]] =
    errorHandler.orElse(ErrorConverter.default).andThen(Source.failed(_))
}
