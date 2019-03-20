package com.emarsys.rdb.connector.common.defaults

import java.sql.{SQLException, SQLSyntaxErrorException}
import java.util.concurrent.{RejectedExecutionException, TimeoutException}

import com.emarsys.rdb.connector.common.models.Errors._
import org.scalatest.{Matchers, WordSpecLike}

class ErrorConverterSpec extends WordSpecLike with Matchers {
  "The default common error converter" should {
    "not touch ConnectorErrors" in {
      ErrorConverter.common(TooManyQueries("a")) shouldBe TooManyQueries("a")
    }

    "convert RejectedExecutionExceptions to TooManyQueries" in {
      ErrorConverter.common(new RejectedExecutionException("msg")) shouldBe TooManyQueries("msg")
    }

    "convert TimeoutException to CompletionTimeout" in {
      ErrorConverter.common(new TimeoutException("msg")) shouldBe CompletionTimeout("msg")
    }

    "convert all other errors to ErrorWithMessage" in {
      ErrorConverter.common(new Exception("msg")) shouldBe ErrorWithMessage("java.lang.Exception: msg")
    }
  }

  "The default SQL error converter" should {
    "recognize syntax errors" in {
      ErrorConverter.sql(new SQLSyntaxErrorException("msg")) shouldBe SqlSyntaxError("msg")
    }

    "recognize comms. link failres" in {
      val message = "Communications link failure - the last packet..."
      ErrorConverter.sql(new SQLException(message, "08S01")) shouldBe CommunicationsLinkFailure(message)
    }

    "rephrase SQLExceptions" in {
      ErrorConverter.sql(new SQLException("msg", "state")) shouldBe ErrorWithMessage("[state] - msg")
    }

    "recognize stucked pools" in {
      val msg = "Task ... rejected from slick.util.AsyncExecutor...[Running, .... active threads = 0, ...]"
      ErrorConverter.sql(new RejectedExecutionException(msg)) shouldBe StuckPool(msg)
    }
  }
}
