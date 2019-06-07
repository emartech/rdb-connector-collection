package com.emarsys.rdb.connector.postgresql

import java.sql.{SQLException, SQLTransientConnectionException}

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.defaults.ErrorConverter
import com.emarsys.rdb.connector.common.models.Errors._

trait PostgreSqlErrorHandling {
  import ErrorConverter._

  val PSQL_STATE_QUERY_CANCELLED             = "57014"
  val PSQL_STATE_SYNTAX_ERROR                = "42601"
  val PSQL_STATE_COLUMN_NOT_FOUND            = "42703"
  val PSQL_STATE_INVALID_TEXT_REPRESENTATION = "22P02"
  val PSQL_STATE_PERMISSION_DENIED           = "42501"

  val PSQL_STATE_RELATION_NOT_FOUND      = "42P01"
  val PSQL_STATE_UNABLE_TO_CONNECT       = "08001"
  val PSQL_AUTHORIZATION_NAME_IS_INVALID = "28000"
  val PSQL_SERVER_PROCESS_IS_TERMINATING = "08006"
  val PSQL_INVALID_PASSWORD              = "28P01"

  val syntaxErrors = Seq(
    PSQL_STATE_SYNTAX_ERROR,
    PSQL_STATE_COLUMN_NOT_FOUND,
    PSQL_STATE_INVALID_TEXT_REPRESENTATION
  )

  val connectionErrors = Seq(
    PSQL_STATE_UNABLE_TO_CONNECT,
    PSQL_AUTHORIZATION_NAME_IS_INVALID,
    PSQL_SERVER_PROCESS_IS_TERMINATING,
    PSQL_INVALID_PASSWORD
  )

  private def errorHandler(): PartialFunction[Throwable, ConnectorError] = {
    case ex: slick.SlickException =>
      if (ex.getMessage == "Update statements should not return a ResultSet") {
        SqlSyntaxError("Wrong update statement: non update query given")
      } else {
        ErrorWithMessage(getErrorMessage(ex))
      }
    case ex: SQLTransientConnectionException if ex.getMessage.contains("timed out") =>
      ConnectionTimeout(getErrorMessage(ex))
    case ex: SQLException if ex.getSQLState == PSQL_STATE_QUERY_CANCELLED    => QueryTimeout(getErrorMessage(ex))
    case ex: SQLException if syntaxErrors.contains(ex.getSQLState)           => SqlSyntaxError(getErrorMessage(ex))
    case ex: SQLException if ex.getSQLState == PSQL_STATE_PERMISSION_DENIED  => AccessDeniedError(getErrorMessage(ex))
    case ex: SQLException if ex.getSQLState == PSQL_STATE_RELATION_NOT_FOUND => TableNotFound(getErrorMessage(ex))
    case ex: SQLException if connectionErrors.contains(ex.getSQLState)       => ConnectionError(ex)
  }

  protected def eitherErrorHandler[T](): PartialFunction[Throwable, Either[ConnectorError, T]] =
    (errorHandler orElse ErrorConverter.default) andThen Left.apply

  protected def streamErrorHandler[A]: PartialFunction[Throwable, Source[A, NotUsed]] =
    (errorHandler orElse ErrorConverter.default) andThen Source.failed

}
