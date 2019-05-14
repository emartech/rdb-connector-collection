package com.emarsys.rdb.connector.mysql

import java.sql.{SQLException, SQLSyntaxErrorException, SQLTransientConnectionException}

import com.emarsys.rdb.connector.common.models.Errors
import com.emarsys.rdb.connector.common.models.Errors._
import com.mysql.cj.jdbc.exceptions.MySQLTimeoutException
import org.scalatest.{Matchers, WordSpecLike}

class MySqlErrorHandlingSpec extends WordSpecLike with Matchers {

  "MySqlErrorHandling" should {
    "convert timeout transient sql error to connection timeout error" in new MySqlErrorHandling {
      val msg = "Connection is not available, request timed out after"
      val e   = new SQLTransientConnectionException(msg)
      eitherErrorHandler.apply(e) shouldEqual Left(ConnectionTimeout(msg))
    }

    "convert lacking privileges error to access denied" in new MySqlErrorHandling {
      val msg = "EXPLAIN/SHOW can not be issued; lacking privileges for underlying table"
      val e   = new SQLException(msg)
      eitherErrorHandler.apply(e) shouldEqual Left(AccessDeniedError(msg))
    }

    "convert sql error to error with message and state if not timeout" in new MySqlErrorHandling {
      val msg = "Other transient error"
      val e   = new SQLTransientConnectionException(msg, "not-handled-sql-state", 999)
      eitherErrorHandler.apply(e) shouldEqual Left(Errors.ErrorWithMessage(s"[not-handled-sql-state] - [999] - $msg"))
    }

    "convert timeout error to query timeout error if query is cancelled" in new MySqlErrorHandling {
      val msg = "Statement cancelled due to timeout or client request"
      val e   = new MySQLTimeoutException(msg)
      eitherErrorHandler.apply(e) shouldEqual Left(QueryTimeout(msg))
    }

    "convert timeout error to connection error if query is not cancelled" in new MySqlErrorHandling {
      val msg = "Other timeout error"
      val e   = new MySQLTimeoutException(msg)
      eitherErrorHandler.apply(e) shouldEqual Left(ConnectionTimeout(msg))
    }

    "convert syntax error exception to access denied error if the message implies that" in new MySqlErrorHandling {
      val msg = "Access denied; you need (at least one of) the PROCESS privilege(s) for this operation"
      val e   = new SQLSyntaxErrorException(msg)
      eitherErrorHandler.apply(e) shouldEqual Left(AccessDeniedError(msg))
    }

    "convert statement closed exception to InvalidDbOperation if the message implies that" in new MySqlErrorHandling {
      val msg = "No operations allowed after statement closed."
      val e   = new SQLException(msg)
      eitherErrorHandler.apply(e) shouldEqual Left(InvalidDbOperation(s"Transient DB error: java.sql.SQLException: $msg"))
    }

    "convert illegal mix of collations error to SqlSyntaxError" in new MySqlErrorHandling {
      val msg = "Illegal mix of collations (utf8mb4_unicode_ci,IMPLICIT) and (utf8mb4_hungarian_ci,IMPLICIT) for operation"
      val e   = new SQLException(msg)
      eitherErrorHandler.apply(e) shouldEqual Left(SqlSyntaxError(s"java.sql.SQLException: $msg"))
    }
  }
}
