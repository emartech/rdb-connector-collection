package com.emarsys.rdb.connector.mssql

import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory => C, ErrorName => N}
import com.microsoft.sqlserver.jdbc.SQLServerException
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor4}
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{EitherValues, PartialFunctionValues}

import java.sql._
import java.util.concurrent.{RejectedExecutionException, TimeoutException}

class MsSqlErrorHandlingSpec
    extends AnyWordSpecLike
    with Matchers
    with EitherValues
    with TableDrivenPropertyChecks
    with PartialFunctionValues {

  private val explainPermissionDenied =
    new SQLException(
      "EXPLAIN/SHOW can not be issued; lacking privileges for underlying table",
      "HY000",
      0,
      new Exception()
    )
  private val showplanException      = new SQLServerException("Error1", "S0004", 0, new Exception())
  private val permissionDenied       = new SQLServerException("Error1", "S0005", 0, new Exception())
  private val invalidObject          = new SQLServerException("Error1", "S0002", 0, new Exception())
  private val duplicateKeyError      = new SQLServerException("Error1", "23000", 0, new Exception())
  private val sqlSyntaxError         = new SQLServerException("Error1", "S0001", 0, new Exception())
  private val queryCancelledError    = new SQLServerException("Error1", "HY008", 0, new Exception())
  private val lackingPrivilegesError = new SQLException("lacking privileges")
  private val invalidUserOrPassword = new SQLException(
    new SQLServerException("Login failed for user", "S0001", 18456, new Exception())
  )

  // format: off
  val mssqlErrorCases: TableFor4[String, Exception, C, N] = Table(
    ("error", "exception", "errorCategory", "errorName"),
    ("query cancelled", queryCancelledError, C.Timeout, N.QueryTimeout),
    ("sql syntax error", sqlSyntaxError, C.FatalQueryExecution, N.SqlSyntaxError),
    ("duplicate key in object", duplicateKeyError, C.FatalQueryExecution, N.SqlSyntaxError),
    ("invalid object name", invalidObject, C.FatalQueryExecution, N.TableNotFound),
    ("permission denied", permissionDenied, C.FatalQueryExecution, N.AccessDeniedError),
    ("showplan permission denied", showplanException, C.FatalQueryExecution, N.AccessDeniedError),
    ("explain permission denied", explainPermissionDenied, C.FatalQueryExecution, N.AccessDeniedError),
    ("lacking privileges error", lackingPrivilegesError, C.FatalQueryExecution, N.AccessDeniedError),
    ("invalid user or password used", invalidUserOrPassword, C.FatalQueryExecution, N.AccessDeniedError)
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

  "ErrorHandling" should {

    forAll(mssqlErrorCases ++ sqlErrorCases ++ commonErrorCases) { case (error, exception, errorCategory, errorName) =>
      s"convert $error to ${errorCategory}#$errorName" in new MsSqlErrorHandling {
        val expectedDatabaseError = DatabaseError(errorCategory, errorName, exception)

        eitherErrorHandler().valueAt(exception).left.value shouldBe expectedDatabaseError
      }
    }

    "compose a meaningful error message of unknown SQLExceptions" in new MsSqlErrorHandling {
      val e = new SQLException("We have no idea", "random-sql-state", 999)
      val expectedDatabaseError =
        DatabaseError(C.Unknown, N.Unknown, "[random-sql-state] - [999] - We have no idea", Some(e), None)

      eitherErrorHandler().valueAt(e).left.value shouldBe expectedDatabaseError
    }

    "not convert DatabaseErrors" in new MsSqlErrorHandling {
      val databaseError = DatabaseError(C.Unknown, N.Unknown, "whatever", None, None)

      eitherErrorHandler().valueAt(databaseError).left.value shouldBe databaseError
    }
  }

  "#onDuplicateKey" should {

    "return default value when duplicate key error happens" in new MsSqlErrorHandling {
      private val defaultValue = "default_value"

      onDuplicateKey(defaultValue).valueAt(duplicateKeyError) shouldBe defaultValue
    }
  }
}
