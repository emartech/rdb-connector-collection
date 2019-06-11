package com.emarsys.rdb.connector.mysql

import java.sql.{SQLException, SQLSyntaxErrorException, SQLTransientConnectionException}

import com.emarsys.rdb.connector.common.models.Errors
import com.emarsys.rdb.connector.common.models.Errors._
import com.mysql.cj.exceptions.MysqlErrorNumbers
import com.mysql.cj.jdbc.exceptions.MySQLTimeoutException
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Matchers, WordSpecLike}

class MySqlErrorHandlingSpec extends WordSpecLike with Matchers with TableDrivenPropertyChecks {

  "MySqlErrorHandling" should {
    "convert timeout transient sql error to connection timeout error" in new MySqlErrorHandling {
      val msg = "Connection is not available, request timed out after"
      val e   = new SQLTransientConnectionException(msg)
      eitherErrorHandler().apply(e) shouldEqual Left(ConnectionTimeout(msg))
    }

    "convert lacking privileges error to access denied" in new MySqlErrorHandling {
      val msg = "EXPLAIN/SHOW can not be issued; lacking privileges for underlying table"
      val e   = new SQLException(msg)
      eitherErrorHandler().apply(e) shouldEqual Left(AccessDeniedError(msg))
    }

    "convert sql error to error with message and state if not timeout" in new MySqlErrorHandling {
      val msg = "Other transient error"
      val e   = new SQLTransientConnectionException(msg, "not-handled-sql-state", 999)
      eitherErrorHandler().apply(e) shouldEqual Left(Errors.ErrorWithMessage(s"[not-handled-sql-state] - [999] - $msg"))
    }

    "convert timeout error to query timeout error if query is cancelled" in new MySqlErrorHandling {
      val msg = "Statement cancelled due to timeout or client request"
      val e   = new MySQLTimeoutException(msg)
      eitherErrorHandler().apply(e) shouldEqual Left(QueryTimeout(msg))
    }

    "convert timeout error to connection error if query is not cancelled" in new MySqlErrorHandling {
      val msg = "Other timeout error"
      val e   = new MySQLTimeoutException(msg)
      eitherErrorHandler().apply(e) shouldEqual Left(ConnectionTimeout(msg))
    }

    "convert syntax error exception to access denied error if the message implies that" in new MySqlErrorHandling {
      val msg = "Access denied; you need (at least one of) the PROCESS privilege(s) for this operation"
      val e   = new SQLSyntaxErrorException(msg)
      eitherErrorHandler().apply(e) shouldEqual Left(AccessDeniedError(msg))
    }

    "convert statement closed exception to TransientDbError if the message implies that" in new MySqlErrorHandling {
      val msg = "No operations allowed after statement closed."
      val e   = new SQLException(msg)
      eitherErrorHandler().apply(e) shouldEqual Left(TransientDbError(msg))
    }

    "convert lock wait timeout exception to TransientDbError if the message implies that" in new MySqlErrorHandling {
      val msg = "Lock wait timeout exceeded; try restarting transaction"
      val e = new SQLException(
        msg,
        MysqlErrorNumbers.SQL_STATE_ROLLBACK_SERIALIZATION_FAILURE,
        MysqlErrorNumbers.ER_LOCK_WAIT_TIMEOUT
      )
      eitherErrorHandler().apply(e) shouldEqual Left(TransientDbError(msg))
    }

    "convert illegal mix of collations error to SqlSyntaxError" in new MySqlErrorHandling {
      val msg =
        "Illegal mix of collations (utf8mb4_unicode_ci,IMPLICIT) and (utf8mb4_hungarian_ci,IMPLICIT) for operation"
      val e = new SQLException(msg)
      eitherErrorHandler().apply(e) shouldEqual Left(SqlSyntaxError(msg))
    }

    "convert host connection error to ConnectionTimeout" in new MySqlErrorHandling {
      val msg = """Can't connect to MySQL server on 'randomaddress.net' (110 "Connection timed out")"""
      val e   = new SQLException(msg)
      eitherErrorHandler().apply(e) shouldEqual Left(ConnectionTimeout(msg))
    }
  }
}
