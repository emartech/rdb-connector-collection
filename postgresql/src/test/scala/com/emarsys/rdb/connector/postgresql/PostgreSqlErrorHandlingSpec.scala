package com.emarsys.rdb.connector.postgresql

import java.sql._
import java.util.concurrent.{RejectedExecutionException, TimeoutException}

import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory => C, ErrorName => N}
import org.postgresql.util.{PSQLException, PSQLState}
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor4}
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{EitherValues, PartialFunctionValues}
import slick.SlickException

class PostgreSqlErrorHandlingSpec
    extends AnyWordSpecLike
    with Matchers
    with TableDrivenPropertyChecks
    with EitherValues
    with PartialFunctionValues {

  private val slickExceptionWrongUpdate          = new SlickException("Update statements should not return a ResultSet")
  private val slickExceptionUnknown              = new SlickException("Unknown")
  private val queryTimeoutException              = new SQLException("msg", "57014")
  private val syntaxErrorException               = new SQLException("msg", "42601")
  private val columnNotFoundException            = new SQLException("msg", "42703")
  private val invalidTextRepresentationException = new SQLException("msg", "22P02")
  private val permissionDeniedException          = new SQLException("msg", "42501")
  private val tableNotFoundException             = new SQLException("msg", "42P01")
  private val invalidAuthorizationException      = new SQLException("msg", "28000")
  private val invalidPasswordException           = new SQLException("msg", "28P01")
  private val cancellingStatementException       = new SQLException("canceling statement...", "40001")
  private val dateOutOfRange                     = new PSQLException("ERROR: date out of range for timestamp", PSQLState.DATETIME_OVERFLOW)

  // format: off
  private val postgresTestCases: TableFor4[String, Exception, C, N] = Table(
    ("error", "exception", "errorCategory", "errorName"),
    ("SlickException with wrong update statement", slickExceptionWrongUpdate, C.FatalQueryExecution, N.SqlSyntaxError),
    ("unknown SlickExceptions", slickExceptionUnknown, C.Unknown, N.Unknown),
    ("query timeout", queryTimeoutException, C.Timeout, N.QueryTimeout),
    ("syntax error", syntaxErrorException, C.FatalQueryExecution, N.SqlSyntaxError),
    ("column not found error", columnNotFoundException, C.FatalQueryExecution, N.SqlSyntaxError),
    ("invalid text representation error", invalidTextRepresentationException, C.FatalQueryExecution, N.SqlSyntaxError),
    ("permission denied error", permissionDeniedException, C.FatalQueryExecution, N.AccessDeniedError),
    ("table not found error", tableNotFoundException, C.FatalQueryExecution, N.TableNotFound),
    ("invalid authorization error", invalidAuthorizationException, C.FatalQueryExecution, N.AccessDeniedError),
    ("invalid password error", invalidPasswordException, C.FatalQueryExecution, N.AccessDeniedError),
    ("canceling statement due to conflict with recovery", cancellingStatementException, C.Transient, N.TransientDbError),
    ("data out of range", dateOutOfRange, C.FatalQueryExecution, N.SqlSyntaxError)
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

  "PostgreSqlErrorHandling" should {

    forAll(postgresTestCases ++ sqlErrorCases ++ commonErrorCases) {
      case (error, exception, errorCategory, errorName) =>
        s"convert $error to $errorCategory#$errorName" in new PostgreSqlErrorHandling {
          val expectedDatabaseError = DatabaseError(errorCategory, errorName, exception)

          eitherErrorHandler().valueAt(exception).left.value shouldBe expectedDatabaseError
        }
    }

    "compose a meaningful error message of unknown SQLExceptions" in new PostgreSqlErrorHandling {
      val e = new SQLException("We have no idea", "random-sql-state", 999)
      val expectedDatabaseError =
        DatabaseError(C.Unknown, N.Unknown, "[random-sql-state] - [999] - We have no idea", Some(e), None)

      eitherErrorHandler().valueAt(e).left.value shouldBe expectedDatabaseError
    }

    "not convert DatabaseErrors" in new PostgreSqlErrorHandling {
      val databaseError = DatabaseError(C.Unknown, N.Unknown, "whatever", None, None)

      eitherErrorHandler().valueAt(databaseError).left.value shouldBe databaseError
    }
  }
}
