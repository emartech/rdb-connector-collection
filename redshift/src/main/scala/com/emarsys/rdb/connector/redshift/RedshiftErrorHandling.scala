package com.emarsys.rdb.connector.redshift

import java.sql.SQLException
import java.util.concurrent.RejectedExecutionException

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.models.Errors.{TooManyQueries, _}

trait RedshiftErrorHandling {
  val REDSHIFT_STATE_CONNECTION_TIMEOUT = "HY000"
  val REDSHIFT_STATE_QUERY_CANCELLED    = "57014"
  val REDSHIFT_STATE_SYNTAX_ERROR       = "42601"
  val REDSHIFT_STATE_PERMISSION_DENIED  = "42501"
  val REDSHIFT_STATE_RELATION_NOT_FOUND = "42P01"

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

  private def errorHandler: PartialFunction[Throwable, ConnectorError] = {
    case ex: RejectedExecutionException                                          => TooManyQueries(ex.getMessage)
    case ex: SQLException if ex.getSQLState == REDSHIFT_STATE_CONNECTION_TIMEOUT => ConnectionTimeout(ex.getMessage)
    case ex: SQLException if ex.getSQLState == REDSHIFT_STATE_QUERY_CANCELLED    => QueryTimeout(ex.getMessage)
    case ex: SQLException if ex.getSQLState == REDSHIFT_STATE_SYNTAX_ERROR       => SqlSyntaxError(ex.getMessage)
    case ex: SQLException if ex.getSQLState == REDSHIFT_STATE_PERMISSION_DENIED  => AccessDeniedError(ex.getMessage)
    case ex: SQLException if ex.getSQLState == REDSHIFT_STATE_RELATION_NOT_FOUND => TableNotFound(ex.getMessage)
    case ex: SQLException if connectionErrors.contains(ex.getSQLState)           => ConnectionError(ex)

    case ex: SQLException => ErrorWithMessage(s"[${ex.getSQLState}] ${ex.getMessage}")
    case ex: Exception    => ErrorWithMessage(ex.getMessage)
  }

  protected def eitherErrorHandler[T]: PartialFunction[Throwable, Either[ConnectorError, T]] =
    errorHandler andThen Left.apply

  protected def streamErrorHandler[A]: PartialFunction[Throwable, Source[A, NotUsed]] =
    errorHandler andThen Source.failed
}
