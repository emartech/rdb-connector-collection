package com.emarsys.rdb.connector.bigquery

import com.emarsys.rdb.connector.common.models.Errors.QueryTimeout
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.TimeoutException

class BigQueryErrorHandlingSpec extends WordSpecLike with Matchers {

  "BigQueryErrorHandling" should {
    "convert timeout exception to QueryTimeout" in new BigQueryErrorHandling {
      val exception = new TimeoutException("msg")
      eitherErrorHandler.apply(exception) shouldEqual Left(QueryTimeout("msg"))
    }
  }
}
