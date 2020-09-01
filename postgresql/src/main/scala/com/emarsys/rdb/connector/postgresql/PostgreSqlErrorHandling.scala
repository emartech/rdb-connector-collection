package com.emarsys.rdb.connector.postgresql

import java.sql.SQLException

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.defaults.ErrorConverter.default
import com.emarsys.rdb.connector.common.models.Errors._

trait PostgreSqlErrorHandling {

  private val PSQL_STATE_QUERY_CANCELLED    = "57014"
  private val PSQL_STATE_RELATION_NOT_FOUND = "42P01"

  private val PSQL_STATE_SYNTAX_ERROR                = "42601"
  private val PSQL_STATE_COLUMN_NOT_FOUND            = "42703"
  private val PSQL_STATE_INVALID_TEXT_REPRESENTATION = "22P02"
  private val syntaxErrors =
    Seq(PSQL_STATE_SYNTAX_ERROR, PSQL_STATE_COLUMN_NOT_FOUND, PSQL_STATE_INVALID_TEXT_REPRESENTATION)

  private val PSQL_INSUFFICIENT_PRIVILEGE        = "42501"
  private val PSQL_AUTHORIZATION_NAME_IS_INVALID = "28000"
  private val PSQL_INVALID_PASSWORD              = "28P01"
  private val accessDeniedErrors =
    Seq(PSQL_INSUFFICIENT_PRIVILEGE, PSQL_AUTHORIZATION_NAME_IS_INVALID, PSQL_INVALID_PASSWORD)

  private def errorHandler(): PartialFunction[Throwable, DatabaseError] = {
    case ex: slick.SlickException if selectQueryWasGivenAsUpdate(ex.getMessage) =>
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SqlSyntaxError, ex)
    case ex: SQLException if ex.getSQLState == PSQL_STATE_QUERY_CANCELLED =>
      DatabaseError(ErrorCategory.Timeout, ErrorName.QueryTimeout, ex)
    case ex: SQLException if syntaxErrors.contains(ex.getSQLState) =>
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SqlSyntaxError, ex)
    case ex: SQLException if accessDeniedErrors.contains(ex.getSQLState) =>
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.AccessDeniedError, ex)
    case ex: SQLException if ex.getSQLState == PSQL_STATE_RELATION_NOT_FOUND =>
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.TableNotFound, ex)
  }

  private def selectQueryWasGivenAsUpdate(message: String) =
    "Update statements should not return a ResultSet" == message

  protected def eitherErrorHandler[T](): PartialFunction[Throwable, Either[DatabaseError, T]] =
    errorHandler.orElse(default).andThen(Left.apply(_))

  protected def streamErrorHandler[A]: PartialFunction[Throwable, Source[A, NotUsed]] =
    errorHandler.orElse(default).andThen(Source.failed(_))

}
