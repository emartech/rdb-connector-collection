package com.emarsys.rdb.connector.mysql

import java.sql.{SQLException, SQLSyntaxErrorException, SQLTransientConnectionException}

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.defaults.ErrorConverter
import com.emarsys.rdb.connector.common.models.Errors._
import com.mysql.cj.jdbc.exceptions.MySQLTimeoutException

trait MySqlErrorHandling {
  import ErrorConverter._

  val MYSQL_EXPLAIN_PERMISSION_DENIED = "EXPLAIN/SHOW can not be issued; lacking privileges for underlying table"
  val MYSQL_STATEMENT_CLOSED          = "No operations allowed after statement closed."
  val MYSQL_ILLEGAL_MIX_OF_COLLATIONS = "Illegal mix of collations"
  val MYSQL_CONNECTION_HOST_ERROR     = "Can't connect to MySQL server on"

  protected def handleNotExistingTable[T](
      table: String
  ): PartialFunction[Throwable, Either[ConnectorError, T]] = {
    case e: Exception if e.getMessage.contains("doesn't exist") =>
      Left(TableNotFound(table))
  }

  private def errorHandler: PartialFunction[Throwable, ConnectorError] = {
    case ex: slick.SlickException =>
      if (ex.getMessage == "Update statements should not return a ResultSet") {
        SqlSyntaxError("Wrong update statement: non update query given")
      } else {
        ErrorWithMessage(getErrorMessage(ex))
      }
    case ex: SQLSyntaxErrorException if ex.getMessage.contains("Access denied") =>
      AccessDeniedError(getErrorMessage(ex))
    case ex: MySQLTimeoutException if ex.getMessage.contains("cancelled") => QueryTimeout(getErrorMessage(ex))
    case ex: MySQLTimeoutException                                        => ConnectionTimeout(getErrorMessage(ex))
    case ex: SQLTransientConnectionException if ex.getMessage.contains("timed out") =>
      ConnectionTimeout(getErrorMessage(ex))
    case ex: SQLException if ex.getMessage.contains(MYSQL_EXPLAIN_PERMISSION_DENIED) =>
      AccessDeniedError(getErrorMessage(ex))
    case ex: SQLException if ex.getMessage.contains(MYSQL_STATEMENT_CLOSED) =>
      InvalidDbOperation(s"Transient DB error: ${ex.toString}")
    case ex: SQLException if ex.getMessage.startsWith(MYSQL_ILLEGAL_MIX_OF_COLLATIONS) =>
      SqlSyntaxError(getErrorMessage(ex))
    case ex: SQLException if ex.getMessage.contains(MYSQL_CONNECTION_HOST_ERROR) =>
      ConnectionTimeout(getErrorMessage(ex))
  }

  protected def eitherErrorHandler[T](): PartialFunction[Throwable, Either[ConnectorError, T]] =
    (errorHandler orElse default) andThen Left.apply

  protected def streamErrorHandler[A]: PartialFunction[Throwable, Source[A, NotUsed]] =
    (errorHandler orElse default) andThen Source.failed

}
