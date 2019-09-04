package com.emarsys.rdb.connector.common.defaults

import java.sql.{SQLException, SQLSyntaxErrorException}
import java.util.concurrent.{RejectedExecutionException, TimeoutException}

import com.emarsys.rdb.connector.common.models.Errors._
import org.scalatest.concurrent.{Signaler, ThreadSignaler, TimeLimits}
import org.scalatest.time.{Milliseconds, Span}
import org.scalatest.{Matchers, WordSpecLike}

class ErrorConverterSpec extends WordSpecLike with Matchers with TimeLimits {

  private def shouldBeWithCause(result: ConnectorError, expected: ConnectorError, expectedCause: Throwable): Unit = {
    result shouldBe expected
    result.getCause shouldBe expectedCause
  }

  "The default common error converter" should {
    "not touch ConnectorErrors" in {
      val exception = TooManyQueries("a")
      shouldBeWithCause(ErrorConverter.common(exception), TooManyQueries("a"), expectedCause = null)
    }

    "convert RejectedExecutionExceptions to TooManyQueries" in {
      val exception = new RejectedExecutionException("msg")
      shouldBeWithCause(ErrorConverter.common(exception), TooManyQueries("msg"), exception)
    }

    "convert TimeoutException to CompletionTimeout" in {
      val exception = new TimeoutException("msg")
      shouldBeWithCause(ErrorConverter.common(exception), CompletionTimeout("msg"), exception)
    }

    "convert all other errors to ErrorWithMessage" in {
      val exception = new RuntimeException("msg")
      shouldBeWithCause(ErrorConverter.common(exception), ErrorWithMessage("msg"), exception)
    }
  }

  "The default SQL error converter" should {
    "recognize syntax errors" in {
      val exception = new SQLSyntaxErrorException("msg")
      shouldBeWithCause(ErrorConverter.sql(exception), SqlSyntaxError("msg"), exception)
    }

    "recognize comms. link failures" in {
      val message   = "Communications link failure - the last packet..."
      val exception = new SQLException(message, "08S01")
      shouldBeWithCause(ErrorConverter.sql(exception), CommunicationsLinkFailure(message), exception)
    }

    "rephrase SQLExceptions" in {
      val exception = new SQLException("msg", "state", 999)
      shouldBeWithCause(ErrorConverter.sql(exception), ErrorWithMessage("[state] - [999] - msg"), exception)
    }

    "recognize stuck pools" in {
      val msg       = "Task ... rejected from slick.util.AsyncExecutor...[Running, .... active threads = 0, ...]"
      val exception = new RejectedExecutionException(msg)
      shouldBeWithCause(ErrorConverter.sql(exception), StuckPool(msg), exception)
    }
  }

  "generate composite message from exception with causes" in {
    val cause     = new RuntimeException("Serious error")
    val exception = new Exception("Grievous error", cause)
    shouldBeWithCause(
      ErrorConverter.default(exception),
      ErrorWithMessage(s"Grievous error\nCaused by: Serious error"),
      exception
    )
  }

  "avoid infinite loop when exception cause is cyclic" in {
    implicit val signaler: Signaler = ThreadSignaler
    val cause                       = new RuntimeException("Serious error")
    val exception                   = new Exception("Grievous error", cause)
    cause.initCause(exception)
    failAfter(Span(10, Milliseconds)) {
      shouldBeWithCause(
        ErrorConverter.default(exception),
        ErrorWithMessage(s"Grievous error\nCaused by: Serious error"),
        exception
      )
    }
  }
}
