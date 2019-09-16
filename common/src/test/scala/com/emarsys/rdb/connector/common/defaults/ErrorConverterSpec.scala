package com.emarsys.rdb.connector.common.defaults

import java.sql.{SQLException, SQLSyntaxErrorException, SQLTransientConnectionException}
import java.util.concurrent.{RejectedExecutionException, TimeoutException}

import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory => C, ErrorName => N}
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor4}
import org.scalatest.{Matchers, PartialFunctionValues, WordSpecLike}

class ErrorConverterSpec extends WordSpecLike with Matchers with PartialFunctionValues with TableDrivenPropertyChecks {

  val sqlErrorCases: TableFor4[String, Exception, C, N] = Table(
    ("error", "exception", "errorCategory", "errorName"),
    ("rejected with no active threads", new RejectedExecutionException("active threads = 0"), C.RateLimit, N.StuckPool),
    ("any sql syntax error", new SQLSyntaxErrorException("nope"), C.FatalQueryExecution, N.SqlSyntaxError),
    ("comm link failure", new SQLException("Communications link failure"), C.Transient, N.CommunicationsLinkFailure),
    ("transient connection times out", new SQLTransientConnectionException("timed out"), C.Timeout, N.ConnectionTimeout)
  )

  val commonErrorCases: TableFor4[String, Exception, C, N] = Table(
    ("error", "exception", "errorCategory", "errorName"),
    ("RejectedExecution", new RejectedExecutionException("this one is not stuck"), C.RateLimit, N.TooManyQueries),
    ("timeout exception", new TimeoutException("Something timed out."), C.Timeout, N.CompletionTimeout),
    ("every other exception", new RuntimeException("Explosion"), C.Unknown, N.Unknown)
  )

  def testConverter(cases: TableFor4[String, Exception, C, N], handler: PartialFunction[Throwable, DatabaseError]) = {
    forAll(cases) {
      case (error, exception, errorCategory, errorName) =>
        s"convert $error to ${errorCategory}#$errorName" in {
          val expectedDatabaseError = DatabaseError(errorCategory, errorName, exception)

          handler.valueAt(exception) shouldBe expectedDatabaseError
        }
    }
  }

  "The default common error converter" should {
    "not touch ConnectorErrors" in {
      val error = DatabaseError(C.RateLimit, N.TooManyQueries, "hohohooo")
      ErrorConverter.common(error) shouldBe error
    }

    testConverter(commonErrorCases, ErrorConverter.common)
  }

  "The default SQL error converter" should {
    "compose a meaningful error message of unknown SQLExceptions" in {
      val e = new SQLException("We have no idea", "random-sql-state", 999)
      val expectedDatabaseError =
        DatabaseError(C.Unknown, N.Unknown, "[random-sql-state] - [999] - We have no idea", Some(e), None)

      ErrorConverter.sql.valueAt(e) shouldBe expectedDatabaseError
    }

    testConverter(sqlErrorCases, ErrorConverter.sql)
  }

  "getCauseMessages" should {
    "return empty list if there is no cause" in {
      val e = new Exception("exception1")
      ErrorConverter.getCauseMessages(e) shouldBe List()
    }

    "return list with a single element if there is a cause without cause" in {
      val cause1 = new Exception("cause1")
      val e      = new Exception("exception1", cause1)
      ErrorConverter.getCauseMessages(e) shouldBe List("cause1")
    }

    "return list with multiple element if there is a cause with cause" in {
      val cause2 = new Exception("cause2")
      val cause1 = new Exception("cause1", cause2)
      val e      = new Exception("exception1", cause1)
      ErrorConverter.getCauseMessages(e) shouldBe List("cause1", "cause2")
    }

    "not stop collect causes if a repeated cause appears based on message only but filter out the duplicated message" in {
      val cause4 = new Exception("cause4")
      val cause3 = new Exception("cause1", cause4)
      val cause2 = new Exception("cause2", cause3)
      val cause1 = new Exception("cause1", cause2)
      val e      = new Exception("exception1", cause1)
      ErrorConverter.getCauseMessages(e) shouldBe List("cause1", "cause2", "cause4")
    }

    "avoid infinite loop when exception cause is cyclic" in {
      val cause3 = new Exception("cause1")
      val cause2 = new Exception("cause2", cause3)
      val cause1 = new Exception("cause1", cause2)
      cause3.initCause(cause1)
      val e = new Exception("exception1", cause1)
      ErrorConverter.getCauseMessages(e) shouldBe List("cause1", "cause2")
    }
  }

}
