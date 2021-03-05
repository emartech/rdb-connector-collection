package com.emarsys.rdb.connector.postgresql

import java.sql.SQLException

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.defaults.ErrorConverter.default
import com.emarsys.rdb.connector.common.models.Errors._
import org.postgresql.util.{PSQLException, PSQLState}

trait PostgreSqlErrorHandling {

  private val PSQL_STATE_INVALID_TEXT_REPRESENTATION = "22P02"
  private val syntaxErrors = Seq(
    PSQLState.SYNTAX_ERROR.getState,
    PSQLState.UNDEFINED_COLUMN.getState,
    PSQL_STATE_INVALID_TEXT_REPRESENTATION
  )

  private val PSQL_INSUFFICIENT_PRIVILEGE        = "42501"
  private val accessDeniedErrors = Seq(
    PSQL_INSUFFICIENT_PRIVILEGE,
    PSQLState.INVALID_AUTHORIZATION_SPECIFICATION.getState,
    PSQLState.INVALID_PASSWORD.getState
  )

  private val PSQL_CANCELLING_STATEMENT_DUE_TO_RECOVERY = "40001"
  private val transientErrors =
    Seq(PSQL_CANCELLING_STATEMENT_DUE_TO_RECOVERY)

  private def errorHandler(): PartialFunction[Throwable, DatabaseError] = {
    case ex: slick.SlickException if selectQueryWasGivenAsUpdate(ex.getMessage) =>
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SqlSyntaxError, ex)
    case ex: SQLException if ex.getSQLState == PSQLState.QUERY_CANCELED.getState =>
      DatabaseError(ErrorCategory.Timeout, ErrorName.QueryTimeout, ex)
    case ex: SQLException if syntaxErrors.contains(ex.getSQLState) =>
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SqlSyntaxError, ex)
    case ex: SQLException if accessDeniedErrors.contains(ex.getSQLState) =>
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.AccessDeniedError, ex)
    case ex: SQLException if transientErrors.contains(ex.getSQLState) =>
      DatabaseError(ErrorCategory.Transient, ErrorName.TransientDbError, ex)
    case ex: SQLException if ex.getSQLState == PSQLState.UNDEFINED_TABLE.getState =>
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.TableNotFound, ex)
    case ex: PSQLException if ex.getSQLState == PSQLState.DATETIME_OVERFLOW.getState =>
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SqlSyntaxError, ex)
  }

  private def selectQueryWasGivenAsUpdate(message: String) =
    "Update statements should not return a ResultSet" == message

  protected def eitherErrorHandler[T](): PartialFunction[Throwable, Either[DatabaseError, T]] =
    errorHandler.orElse(default).andThen(Left.apply(_))

  protected def streamErrorHandler[A]: PartialFunction[Throwable, Source[A, NotUsed]] =
    errorHandler.orElse(default).andThen(Source.failed(_))

}
