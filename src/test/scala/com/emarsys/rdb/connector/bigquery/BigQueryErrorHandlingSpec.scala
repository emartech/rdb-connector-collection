package com.emarsys.rdb.connector.bigquery

import java.util.concurrent.RejectedExecutionException

import com.emarsys.rdb.connector.common.models.Errors.{ErrorWithMessage, QueryTimeout, TableNotFound, TooManyQueries}
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.TimeoutException

class BigQueryErrorHandlingSpec extends WordSpecLike with Matchers {

  "BigQueryErrorHandling" should {
    "throw connection error forward" in new BigQueryErrorHandling {
      val exception = new TableNotFound("")
      eitherErrorHandler.apply(exception) shouldEqual Left(exception)
    }

    "convert timeout exception to QueryTimeout" in new BigQueryErrorHandling {
      val exception = new TimeoutException("msg")
      eitherErrorHandler.apply(exception) shouldEqual Left(QueryTimeout("msg"))
    }

    "convert RejectedExecutionException to TooManyQueries" in new BigQueryErrorHandling {
      val exception = new RejectedExecutionException
      eitherErrorHandler.apply(exception) shouldEqual Left(TooManyQueries)
    }

    "convert unidentified exception to ErrorWithMessage" in new BigQueryErrorHandling {
      val exception = new Exception("msg")
      eitherErrorHandler.apply(exception) shouldEqual Left(ErrorWithMessage("java.lang.Exception: msg"))
    }
  }
}
