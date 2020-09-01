package com.emarsys.rdb.connector.mysql

import java.sql.{SQLException, SQLSyntaxErrorException}

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.defaults.ErrorConverter
import com.emarsys.rdb.connector.common.models.Errors._
import com.mysql.cj.exceptions.MysqlErrorNumbers.SQL_STATE_INSERT_VALUE_LIST_NO_MATCH_COL_LIST
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

  // TODO undo this one
  protected def handleNotExistingTable[T](
      table: String
  ): PartialFunction[Throwable, Either[DatabaseError, T]] = {
    case e: Exception if e.getMessage.contains("doesn't exist") =>
      Left(DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.TableNotFound, e))
  }

  private def errorHandler: PartialFunction[Throwable, DatabaseError] = {
    case ex: slick.SlickException if selectQueryWasGivenAsUpdate(ex.getMessage) =>
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SqlSyntaxError, ex)
    case ex: SQLException if ex.getMessage.contains("Access denied") =>
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.AccessDeniedError, ex)
    case ex: MySQLTimeoutException if ex.getMessage.contains("cancelled") =>
      DatabaseError(ErrorCategory.Timeout, ErrorName.QueryTimeout, ex)
    case ex: MySQLTimeoutException =>
      DatabaseError(ErrorCategory.Timeout, ErrorName.ConnectionTimeout, ex)
    case ex: SQLException if ex.getMessage.contains(MYSQL_EXPLAIN_PERMISSION_DENIED) =>
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.AccessDeniedError, ex)
    case ex: SQLException if isTransientDbError(ex.getMessage) =>
      DatabaseError(ErrorCategory.Transient, ErrorName.TransientDbError, ex)
    case ex: SQLException if isSyntaxError(ex.getMessage) =>
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SqlSyntaxError, ex)
    case ex: SQLException if ex.getSQLState == SQL_STATE_INSERT_VALUE_LIST_NO_MATCH_COL_LIST =>
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SqlSyntaxError, ex)
    case ex: SQLException if ex.getMessage.contains(MYSQL_CONNECTION_HOST_ERROR) =>
      DatabaseError(ErrorCategory.Timeout, ErrorName.ConnectionTimeout, ex)
  }

  private def selectQueryWasGivenAsUpdate(message: String) =
    "Update statements should not return a ResultSet" == message

  private def isTransientDbError(message: String): Boolean =
    List(MYSQL_STATEMENT_CLOSED, MYSQL_LOCK_WAIT_TIMEOUT, MYSQL_CONNECTION_CLOSED, MYSQL_READONLY).exists(
      message.contains
    )

  private def isSyntaxError(message: String): Boolean = {
    message.startsWith(MYSQL_ILLEGAL_MIX_OF_COLLATIONS) ||
    (message.startsWith(MYSQL_VIEW_INVALID_REFERENCE_BEG) && message.endsWith(MYSQL_VIEW_INVALID_REFERENCE_END))
  }

  protected def eitherErrorHandler[T](): PartialFunction[Throwable, Either[DatabaseError, T]] =
    errorHandler.orElse(default).andThen(Left.apply(_))

  protected def streamErrorHandler[A]: PartialFunction[Throwable, Source[A, NotUsed]] =
    errorHandler.orElse(default).andThen(Source.failed(_))

}
