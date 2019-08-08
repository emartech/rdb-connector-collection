package com.emarsys.rdb.connector.mysql

import java.sql.{SQLException, SQLSyntaxErrorException, SQLTransientConnectionException}
import java.util.concurrent.{RejectedExecutionException, TimeoutException}

import com.emarsys.rdb.connector.common.models.Errors._
import com.mysql.cj.exceptions.MysqlErrorNumbers.{ER_LOCK_WAIT_TIMEOUT, SQL_STATE_ROLLBACK_SERIALIZATION_FAILURE}
import com.mysql.cj.jdbc.exceptions.MySQLTimeoutException
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{EitherValues, Matchers, PartialFunctionValues, WordSpecLike}
import slick.SlickException

class MySqlErrorHandlingSpec
    extends WordSpecLike
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
  private val accessDeniedException               = new SQLSyntaxErrorException(accessDeniedMsg)
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

  private val mySqlErrorCases = Table(
    ("errorType", "errorType", "connectorError"),
    (
      "SlickException with wrong update statement",
      slickExceptionWrongUpdate,
      SqlSyntaxError("Wrong update statement: non update query given")
    ),
    ("access denied exception", accessDeniedException, AccessDeniedError(accessDeniedMsg)),
    ("query timeout exception", queryTimeoutException, QueryTimeout(queryTimeoutMsg)),
    (
      "connection time out exception",
      connectionTimeoutException,
      ConnectionTimeout(connectionTimeoutException.getMessage)
    ),
    (
      "no permission for EXPLAIN/SHOW",
      noPermissionForExplainShowException,
      AccessDeniedError(noPermissionForExplainShowMsg)
    ),
    ("statement closed exception", statementClosedException, TransientDbError(statementClosedMsg)),
    ("lock wait timeout exception", lockWaitTimeoutException, TransientDbError(lockWaitTimeoutMsg)),
    ("connection closed exception", connectionClosedException, TransientDbError(connectionClosedMsg)),
    ("db is in read-only mode exception", dbReadOnlyException, TransientDbError(dbIsReadOnlyMsg)),
    ("illegal mix of collation exception", illegalMixOfCollationException, SqlSyntaxError(illegalMixOfCollation)),
    ("host connection error", hostConnectionException, ConnectionTimeout(hostConnectionErrorMsg)),
    ("invalid view exception", invalidViewException, SqlSyntaxError(invalidViewMsg))
  )

  val sqlErrorCases = Table(
    ("errorType", "errorType", "connectorError"),
    (
      "rejected execution with active threads = 0",
      new RejectedExecutionException("active threads = 0"),
      StuckPool("active threads = 0")
    ),
    ("any sql syntax error", new SQLSyntaxErrorException("nope"), SqlSyntaxError("nope")),
    (
      "comm link failure",
      new SQLException("Communications link failure"),
      CommunicationsLinkFailure("Communications link failure")
    ),
    (
      "transient connection times out",
      new SQLTransientConnectionException("timed out"),
      ConnectionTimeout("timed out")
    ),
    (
      "any other sql error",
      new SQLException("We have no idea", "random-sql-state", 999),
      ErrorWithMessage(s"[random-sql-state] - [999] - We have no idea")
    )
  )

  val commonErrorCases = Table(
    ("errorType", "incomingException", "connectorError"),
    (
      "RejectedExecutionException",
      new RejectedExecutionException("this one is not stucked"),
      TooManyQueries("this one is not stucked")
    ),
    ("time out exception", new TimeoutException("Something timed out."), CompletionTimeout("Something timed out.")),
    ("every other exception", new RuntimeException("Explosion"), ErrorWithMessage("Explosion"))
  )

  "MySqlErrorHandling" should {

    "handleNotExistingTable" in new MySqlErrorHandling {
      val table = "tableName"

      handleNotExistingTable(table).valueAt(new Exception("doesn't exist")).left.value shouldBe TableNotFound(table)
    }

    forAll(mySqlErrorCases ++ sqlErrorCases ++ commonErrorCases) {
      case (errorType, incomingException, connectorError) =>
        s"convert $errorType to ${connectorError.getClass.getSimpleName}" in new MySqlErrorHandling {
          val actualError = eitherErrorHandler().valueAt(incomingException)

          actualError.left.value shouldBe connectorError
          actualError.left.value.getCause shouldBe incomingException
        }
    }

    "pass through ConnectorError in common error handling" in new MySqlErrorHandling {
      val connectorError = ConnectionConfigError("config error")

      eitherErrorHandler().valueAt(connectorError).left.value shouldBe connectorError
    }
  }
}
