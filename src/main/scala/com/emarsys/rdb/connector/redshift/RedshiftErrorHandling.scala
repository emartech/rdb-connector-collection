package com.emarsys.rdb.connector.redshift

import java.sql.{SQLException, SQLTransientConnectionException}

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.models.Errors._

trait RedshiftErrorHandling {
  val PSQL_STATE_CONNECTION_TIMEOUT = "HY000"
  val PSQL_STATE_QUERY_CANCELLED    = "57014"
  val PSQL_STATE_SYNTAX_ERROR       = "42601"
  val PSQL_STATE_PERMISSION_DENIED  = "42501"
  val PSQL_STATE_RELATION_NOT_FOUND = "42P01"

  val PSQL_STATE_UNABLE_TO_CONNECT       = "08001"
  val PSQL_AUTHORIZATION_NAME_IS_INVALID = "28000"
  val PSQL_SERVER_PROCESS_IS_TERMINATING = "08006"
  val PSQL_INVALID_PASSWORD              = "28P01"

  val connectionErrors = Seq(
    PSQL_STATE_UNABLE_TO_CONNECT,
    PSQL_AUTHORIZATION_NAME_IS_INVALID,
    PSQL_SERVER_PROCESS_IS_TERMINATING,
    PSQL_INVALID_PASSWORD
  )

  private def errorHandler: PartialFunction[Throwable, ConnectorError] = {
    case ex: SQLException if ex.getSQLState == PSQL_STATE_CONNECTION_TIMEOUT => ConnectionTimeout(ex.getMessage)
    case ex: SQLException if ex.getSQLState == PSQL_STATE_QUERY_CANCELLED    => QueryTimeout(ex.getMessage)
    case ex: SQLException if ex.getSQLState == PSQL_STATE_SYNTAX_ERROR       => SqlSyntaxError(ex.getMessage)
    case ex: SQLException if ex.getSQLState == PSQL_STATE_PERMISSION_DENIED  => AccessDeniedError(ex.getMessage)
    case ex: SQLException if ex.getSQLState == PSQL_STATE_RELATION_NOT_FOUND => TableNotFound(ex.getMessage)
    case ex: SQLException if connectionErrors.contains(ex.getSQLState)       => ConnectionError(ex)

    case ex: SQLException => ErrorWithMessage(s"[${ex.getSQLState}] ${ex.getMessage}")
    case ex: Exception    => ErrorWithMessage(ex.getMessage)
  }

  protected def eitherErrorHandler[T]: PartialFunction[Throwable, Either[ConnectorError, T]] =
    errorHandler andThen Left.apply

  protected def streamErrorHandler[A]: PartialFunction[Throwable, Source[A, NotUsed]] =
    errorHandler andThen Source.failed
}
