package com.emarsys.rdb.connector.mysql

import java.sql.{SQLException, SQLSyntaxErrorException}

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.defaults.ErrorConverter
import com.emarsys.rdb.connector.common.models.Errors._
import com.mysql.cj.jdbc.exceptions.MySQLTimeoutException

trait MySqlErrorHandling {
  import ErrorConverter._

  val MYSQL_EXPLAIN_PERMISSION_DENIED  = "EXPLAIN/SHOW can not be issued; lacking privileges for underlying table"
  val MYSQL_STATEMENT_CLOSED           = "No operations allowed after statement closed."
  val MYSQL_CONNECTION_CLOSED          = "No operations allowed after connection closed."
  val MYSQL_ILLEGAL_MIX_OF_COLLATIONS  = "Illegal mix of collations"
  val MYSQL_VIEW_INVALID_REFERENCE_BEG = "View"
  val MYSQL_VIEW_INVALID_REFERENCE_END =
    "references invalid table(s) or column(s) or function(s) or definer/invoker of view lack rights to use them"
  val MYSQL_CONNECTION_HOST_ERROR = "Can't connect to MySQL server on"
  val MYSQL_LOCK_WAIT_TIMEOUT     = "Lock wait timeout exceeded; try restarting transaction"
  val MYSQL_READONLY              = "The MySQL server is running with the --read-only option"

  protected def handleNotExistingTable[T](
      table: String
  ): PartialFunction[Throwable, Either[ConnectorError, T]] = {
    case e: Exception if e.getMessage.contains("doesn't exist") =>
      Left(TableNotFound(table).withCause(e))
  }

  private def errorHandler: PartialFunction[Throwable, ConnectorError] = {
    case ex: slick.SlickException =>
      if (ex.getMessage == "Update statements should not return a ResultSet") {
        SqlSyntaxError("Wrong update statement: non update query given").withCause(ex)
      } else {
        ErrorWithMessage(getErrorMessage(ex)).withCause(ex)
      }
    case ex: SQLSyntaxErrorException if ex.getMessage.contains("Access denied") =>
      AccessDeniedError(getErrorMessage(ex)).withCause(ex)
    case ex: MySQLTimeoutException if ex.getMessage.contains("cancelled") =>
      QueryTimeout(getErrorMessage(ex)).withCause(ex)
    case ex: MySQLTimeoutException =>
      ConnectionTimeout(getErrorMessage(ex)).withCause(ex)
    case ex: SQLException if ex.getMessage.contains(MYSQL_EXPLAIN_PERMISSION_DENIED) =>
      AccessDeniedError(getErrorMessage(ex)).withCause(ex)
    case ex: SQLException if isTransientDbError(ex.getMessage) =>
      TransientDbError(getErrorMessage(ex)).withCause(ex)
    case ex: SQLException if isSyntaxError(ex.getMessage) =>
      SqlSyntaxError(getErrorMessage(ex)).withCause(ex)
    case ex: SQLException if ex.getMessage.contains(MYSQL_CONNECTION_HOST_ERROR) =>
      ConnectionTimeout(getErrorMessage(ex)).withCause(ex)
  }

  private def isTransientDbError(message: String): Boolean =
    List(MYSQL_STATEMENT_CLOSED, MYSQL_LOCK_WAIT_TIMEOUT, MYSQL_CONNECTION_CLOSED, MYSQL_READONLY).exists(
      message.contains
    )

  private def isSyntaxError(message: String): Boolean = {
    message.startsWith(MYSQL_ILLEGAL_MIX_OF_COLLATIONS) ||
    (message.startsWith(MYSQL_VIEW_INVALID_REFERENCE_BEG) && message.endsWith(MYSQL_VIEW_INVALID_REFERENCE_END))
  }

  protected def eitherErrorHandler[T](): PartialFunction[Throwable, Either[ConnectorError, T]] =
    (errorHandler orElse default) andThen Left.apply

  protected def streamErrorHandler[A]: PartialFunction[Throwable, Source[A, NotUsed]] =
    (errorHandler orElse default) andThen Source.failed

}
