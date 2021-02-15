package com.emarsys.rdb.connector.hana

import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory => C, ErrorName => N}
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor4}
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{EitherValues, PartialFunctionValues}

import java.sql._
import java.util.concurrent.{RejectedExecutionException, TimeoutException}

class HanaErrorHandlingSpec
    extends AnyWordSpecLike
    with Matchers
    with TableDrivenPropertyChecks
    with EitherValues
    with PartialFunctionValues {

  // format: off
  private val hanaErrorCases: TableFor4[String, Exception, C, N] = Table(
    ("error", "exception", "errorCategory", "errorName"),
    ("invalid table name", new SQLException("invalid table name", "", 259), C.FatalQueryExecution, N.TableNotFound),
    ("general error", new SQLException("general error", "", 2), C.Transient, N.TransientDbError),
    ("fatal error", new SQLException("fatal error", "", 3), C.Transient, N.TransientDbError),
    ("invalid license", new SQLException("invalid license", "", 19), C.FatalQueryExecution, N.QueryRejected),
    ("exceed max num of concurrent transactions", new SQLException("exceed max num of concurrent transactions", "", 142), C.RateLimit, N.TooManyQueries),
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

  "HanaErrorHandling" should {

    forAll(hanaErrorCases ++ sqlErrorCases ++ commonErrorCases) { case (error, exception, errorCategory, errorName) =>
      s"convert $error to ${errorCategory}#$errorName" in new HanaErrorHandling {
        val expectedDatabaseError = DatabaseError(errorCategory, errorName, exception)

        eitherErrorHandler().valueAt(exception).left.value shouldBe expectedDatabaseError
      }
    }

    "compose a meaningful error message of unknown SQLExceptions" in new HanaErrorHandling {
      val e = new SQLException("We have no idea", "random-sql-state", 999)
      val expectedDatabaseError =
        DatabaseError(C.Unknown, N.Unknown, "[random-sql-state] - [999] - We have no idea", Some(e), None)

      eitherErrorHandler().valueAt(e).left.value shouldBe expectedDatabaseError
    }

    "not convert DatabaseErrors" in new HanaErrorHandling {
      val databaseError = DatabaseError(C.Unknown, N.Unknown, "whatever", None, None)

      eitherErrorHandler().valueAt(databaseError).left.value shouldBe databaseError
    }
  }
}
