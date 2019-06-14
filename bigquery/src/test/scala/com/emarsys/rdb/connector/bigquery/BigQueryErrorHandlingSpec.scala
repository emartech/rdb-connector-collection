package com.emarsys.rdb.connector.bigquery

import com.emarsys.rdb.connector.common.models.Errors.{ConnectorError, QueryTimeout}
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.TimeoutException

class BigQueryErrorHandlingSpec extends WordSpecLike with Matchers {

  private def shouldBeWithCause[T](
      result: Either[ConnectorError, T],
      expected: ConnectorError,
      expectedCause: Throwable
  ): Unit = {
    result shouldBe Left(expected)
    result.left.get.getCause shouldBe expectedCause
  }

  "BigQueryErrorHandling" should {
    "convert timeout exception to QueryTimeout" in new BigQueryErrorHandling {
      val exception = new TimeoutException("msg")
      shouldBeWithCause(eitherErrorHandler.apply(exception), QueryTimeout("msg"), exception)
    }
  }
}
