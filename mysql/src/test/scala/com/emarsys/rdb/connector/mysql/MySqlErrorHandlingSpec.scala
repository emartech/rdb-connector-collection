package com.emarsys.rdb.connector.mysql

import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory => C, ErrorName => N}
import com.mysql.cj.exceptions.MysqlErrorNumbers._
import com.mysql.cj.jdbc.exceptions.MySQLTimeoutException
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor4}
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{EitherValues, PartialFunctionValues}
import slick.SlickException

import java.sql._
import java.util.concurrent.{RejectedExecutionException, TimeoutException}

class MySqlErrorHandlingSpec
    extends AnyWordSpecLike
    with Matchers
    with TableDrivenPropertyChecks
    with EitherValues
    with PartialFunctionValues {

  private val queryTimeoutMsg               = "Statement cancelled due to timeout or client request"
  private val noPermissionForExplainShowMsg = "EXPLAIN/SHOW can not be issued; lacking privileges for underlying table"
  private val accessDeniedMsg               = "Access denied"
  private val statementClosedMsg            = "No operations allowed after statement closed."
  private val connectionClosedMsg           = "No operations allowed after connection closed."
  private val dbIsReadOnlyMsg =
    "The MySQL server is running with the --read-only option so it cannot execute this statement"
  private val lockWaitTimeoutMsg = "Lock wait timeout exceeded; try restarting transaction"
  private val illegalMixOfCollation =
    "Illegal mix of collations (utf8mb4_unicode_ci,IMPLICIT) and (utf8mb4_hungarian_ci,IMPLICIT) for operation"
  private val hostConnectionErrorMsg =
    """Can't connect to MySQL server on 'randomaddress.net' (110 "Connection timed out")"""
  private val invalidViewMsg =
    """View 'compose.PostPurchaseView' references invalid table(s) or column(s) or function(s) or definer/invoker of view lack rights to use them"""

  private val slickExceptionWrongUpdate           = new SlickException("Update statements should not return a ResultSet")
  private val accessDeniedExceptionSyntaxError    = new SQLSyntaxErrorException(accessDeniedMsg)
  private val accessDeniedExceptionSQLE           = new SQLException(accessDeniedMsg)
  private val queryTimeoutException               = new MySQLTimeoutException(queryTimeoutMsg)
  private val connectionTimeoutException          = new MySQLTimeoutException("Other timeout error")
  private val noPermissionForExplainShowException = new SQLException(noPermissionForExplainShowMsg)
  private val statementClosedException            = new SQLException(statementClosedMsg)
  private val lockWaitTimeoutException =
    new SQLException(lockWaitTimeoutMsg, SQL_STATE_ROLLBACK_SERIALIZATION_FAILURE, ER_LOCK_WAIT_TIMEOUT)
  private val connectionClosedException      = new SQLException(connectionClosedMsg)
  private val dbReadOnlyException            = new SQLException(dbIsReadOnlyMsg)
  private val illegalMixOfCollationException = new SQLException(illegalMixOfCollation)
  private val hostConnectionException        = new SQLException(hostConnectionErrorMsg)
  private val invalidViewException           = new SQLException(invalidViewMsg)
  private val wrongRowCountException =
    new SQLException("whatever", SQL_STATE_INSERT_VALUE_LIST_NO_MATCH_COL_LIST, ER_WRONG_VALUE_COUNT_ON_ROW)
  private val wrongValueCountException =
    new SQLException("whatever", SQL_STATE_INSERT_VALUE_LIST_NO_MATCH_COL_LIST, ER_WRONG_VALUE_COUNT)

  // format: off
  private val mySqlErrorCases: TableFor4[String, Exception, C, N] = Table(
    ("error", "exception", "errorCategory", "errorName"),
    ("SlickException with wrong update statement", slickExceptionWrongUpdate, C.FatalQueryExecution, N.SqlSyntaxError),
    ("access denied exception (SQLSyntaxErrorException)", accessDeniedExceptionSyntaxError, C.FatalQueryExecution, N.AccessDeniedError),
    ("access denied exception (SQLException)", accessDeniedExceptionSQLE, C.FatalQueryExecution, N.AccessDeniedError),
    ("query timeout exception", queryTimeoutException, C.Timeout, N.QueryTimeout),
    ("connection time out exception", connectionTimeoutException, C.Timeout, N.ConnectionTimeout),
    ("no permission for EXPLAIN/SHOW", noPermissionForExplainShowException, C.FatalQueryExecution, N.AccessDeniedError),
    ("statement closed exception", statementClosedException, C.Transient, N.TransientDbError),
    ("lock wait timeout exception", lockWaitTimeoutException, C.Transient, N.TransientDbError),
    ("connection closed exception", connectionClosedException, C.Transient, N.TransientDbError),
    ("db is in read-only mode exception", dbReadOnlyException, C.Transient, N.TransientDbError),
    ("illegal mix of collation exception", illegalMixOfCollationException, C.FatalQueryExecution, N.SqlSyntaxError),
    ("host connection error", hostConnectionException, C.Timeout, N.ConnectionTimeout),
    ("invalid view exception", invalidViewException, C.FatalQueryExecution, N.SqlSyntaxError),
    ("wrong value count on row", wrongRowCountException, C.FatalQueryExecution, N.SqlSyntaxError),
    ("wrong value count", wrongValueCountException, C.FatalQueryExecution, N.SqlSyntaxError)
  )

  private val sqlErrorCases: TableFor4[String, Exception, C, N] = Table(
    ("error", "exception", "errorCategory", "errorName"),
    ("rejected with no active threads", new RejectedExecutionException("active threads = 0"), C.RateLimit, N.StuckPool),
    ("any sql syntax error", new SQLSyntaxErrorException("nope"), C.FatalQueryExecution, N.SqlSyntaxError),
    ("comm link failure", new SQLException("Communications link failure"), C.Transient, N.CommunicationsLinkFailure),
    ("transient connection times out", new SQLTransientConnectionException("timed out"), C.Timeout, N.ConnectionTimeout),
    ("execution aborted by timeout", new SQLTimeoutException("execution aborted by timeout", "", 613), C.Timeout, N.QueryTimeout),
    ("transaction rollback", new SQLTransactionRollbackException("", "", 129), C.Transient, N.TransientDbError),
    ("non transient connection error", new SQLNonTransientConnectionException("", "", 0), C.FatalQueryExecution, N.ConnectionConfigError),
    ("invalid authorization", new SQLInvalidAuthorizationSpecException("", "", 0), C.FatalQueryExecution, N.AccessDeniedError),
  )

  private val commonErrorCases: TableFor4[String, Exception, C, N] = Table(
    ("error", "exception", "errorCategory", "errorName"),
    ("RejectedExecution", new RejectedExecutionException("this one is not stuck"), C.RateLimit, N.TooManyQueries),
    ("timeout exception", new TimeoutException("Something timed out."), C.Timeout, N.CompletionTimeout),
    ("every other exception", new RuntimeException("Explosion"), C.Unknown, N.Unknown)
  )
  // format: on

  "MySqlErrorHandling" should {

    "handleNotExistingTable" in new MySqlErrorHandling {
      val table                 = "tableName"
      val e                     = new Exception("doesn't exist")
      val expectedDatabaseError = DatabaseError(C.FatalQueryExecution, N.TableNotFound, e)

      handleNotExistingTable(table).valueAt(e).left.value shouldBe expectedDatabaseError
    }

    forAll(mySqlErrorCases ++ sqlErrorCases ++ commonErrorCases) { case (error, exception, errorCategory, errorName) =>
      s"convert $error to ${errorCategory}#$errorName" in new MySqlErrorHandling {
        val expectedDatabaseError = DatabaseError(errorCategory, errorName, exception)

        eitherErrorHandler().valueAt(exception).left.value shouldBe expectedDatabaseError
      }
    }

    "compose a meaningful error message of unknown SQLExceptions" in new MySqlErrorHandling {
      val e = new SQLException("We have no idea", "random-sql-state", 999)
      val expectedDatabaseError =
        DatabaseError(C.Unknown, N.Unknown, "[random-sql-state] - [999] - We have no idea", Some(e), None)

      eitherErrorHandler().valueAt(e).left.value shouldBe expectedDatabaseError
    }

    "not convert DatabaseErrors" in new MySqlErrorHandling {
      val databaseError = DatabaseError(C.Unknown, N.Unknown, "whatever", None, None)

      eitherErrorHandler().valueAt(databaseError).left.value shouldBe databaseError
    }
  }
}
