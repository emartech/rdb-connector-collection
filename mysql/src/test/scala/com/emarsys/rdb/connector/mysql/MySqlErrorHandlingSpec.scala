package com.emarsys.rdb.connector.mysql

import java.sql.{SQLException, SQLSyntaxErrorException, SQLTransientConnectionException}

import com.emarsys.rdb.connector.common.models.Errors._
import com.mysql.cj.exceptions.MysqlErrorNumbers
import com.mysql.cj.jdbc.exceptions.MySQLTimeoutException
import org.scalatest.{Matchers, WordSpecLike}
import slick.SlickException

class MySqlErrorHandlingSpec extends WordSpecLike with Matchers {

  private def shouldBeWithCause[T](
      result: Either[ConnectorError, T],
      expected: ConnectorError,
      expectedCause: Throwable
  ): Unit = {
    result shouldBe Left(expected)
    result.left.get.getCause shouldBe expectedCause
  }

  "MySqlErrorHandling" should {
    "convert SlickException with wrong update statement to syntax error" in new MySqlErrorHandling {
      val msg = "Update statements should not return a ResultSet"
      val e   = new SlickException(msg)
      shouldBeWithCause(
        eitherErrorHandler().apply(e),
        SqlSyntaxError("Wrong update statement: non update query given"),
        e
      )
    }

    "convert unknown SlickExceptions to ErrorWithMessage" in new MySqlErrorHandling {
      val msg = "Unknown"
      val e   = new SlickException(msg)
      shouldBeWithCause(eitherErrorHandler().apply(e), ErrorWithMessage(msg), e)
    }

    "convert timeout transient sql error to connection timeout error" in new MySqlErrorHandling {
      val msg = "Connection is not available, request timed out after"
      val e   = new SQLTransientConnectionException(msg)
      shouldBeWithCause(eitherErrorHandler().apply(e), ConnectionTimeout(msg), e)
    }

    "convert lacking privileges error to access denied" in new MySqlErrorHandling {
      val msg = "EXPLAIN/SHOW can not be issued; lacking privileges for underlying table"
      val e   = new SQLException(msg)
      shouldBeWithCause(eitherErrorHandler().apply(e), AccessDeniedError(msg), e)
    }

    "convert sql error to error with message and state if not timeout" in new MySqlErrorHandling {
      val msg = "Other transient error"
      val e   = new SQLTransientConnectionException(msg, "not-handled-sql-state", 999)
      shouldBeWithCause(
        eitherErrorHandler().apply(e),
        ErrorWithMessage(s"[not-handled-sql-state] - [999] - $msg"),
        e
      )
    }

    "convert timeout error to query timeout error if query is cancelled" in new MySqlErrorHandling {
      val msg = "Statement cancelled due to timeout or client request"
      val e   = new MySQLTimeoutException(msg)
      shouldBeWithCause(eitherErrorHandler().apply(e), QueryTimeout(msg), e)
    }

    "convert timeout error to connection error if query is not cancelled" in new MySqlErrorHandling {
      val msg = "Other timeout error"
      val e   = new MySQLTimeoutException(msg)
      shouldBeWithCause(eitherErrorHandler().apply(e), ConnectionTimeout(msg), e)
    }

    "convert syntax error exception to access denied error if the message implies that" in new MySqlErrorHandling {
      val msg = "Access denied; you need (at least one of) the PROCESS privilege(s) for this operation"
      val e   = new SQLSyntaxErrorException(msg)
      shouldBeWithCause(eitherErrorHandler().apply(e), AccessDeniedError(msg), e)
    }

    "convert statement closed exception to TransientDbError if the message implies that" in new MySqlErrorHandling {
      val msg = "No operations allowed after statement closed."
      val e   = new SQLException(msg)
      shouldBeWithCause(eitherErrorHandler().apply(e), TransientDbError(msg), e)
    }

    "convert connection closed exception to TransientDbError if the message implies that" in new MySqlErrorHandling {
      val msg = "No operations allowed after connection closed."
      val e   = new SQLException(msg)
      shouldBeWithCause(eitherErrorHandler().apply(e), TransientDbError(msg), e)
    }

    "convert exceptions raised because of a read-only master to TransientDbError" in new MySqlErrorHandling {
      val msg = "The MySQL server is running with the --read-only option so it cannot execute this statement"
      val e   = new SQLException(msg)
      shouldBeWithCause(eitherErrorHandler().apply(e), TransientDbError(msg), e)
    }

    "convert lock wait timeout exception to TransientDbError if the message implies that" in new MySqlErrorHandling {
      val msg = "Lock wait timeout exceeded; try restarting transaction"
      val e = new SQLException(
        msg,
        MysqlErrorNumbers.SQL_STATE_ROLLBACK_SERIALIZATION_FAILURE,
        MysqlErrorNumbers.ER_LOCK_WAIT_TIMEOUT
      )
      shouldBeWithCause(eitherErrorHandler().apply(e), TransientDbError(msg), e)
    }

    "convert illegal mix of collations error to SqlSyntaxError" in new MySqlErrorHandling {
      val msg =
        "Illegal mix of collations (utf8mb4_unicode_ci,IMPLICIT) and (utf8mb4_hungarian_ci,IMPLICIT) for operation"
      val e = new SQLException(msg)
      shouldBeWithCause(eitherErrorHandler().apply(e), SqlSyntaxError(msg), e)
    }

    "convert host connection error to ConnectionTimeout" in new MySqlErrorHandling {
      val msg = """Can't connect to MySQL server on 'randomaddress.net' (110 "Connection timed out")"""
      val e   = new SQLException(msg)
      shouldBeWithCause(eitherErrorHandler().apply(e), ConnectionTimeout(msg), e)
    }

    "convert invalid view use to SqlSyntaxError" in new MySqlErrorHandling {
      val msg =
        """View 'compose.PostPurchaseView' references invalid table(s) or column(s) or function(s) or definer/invoker of view lack rights to use them"""
      val e = new SQLException(msg)
      shouldBeWithCause(eitherErrorHandler().apply(e), SqlSyntaxError(msg), e)
    }

  }
}
