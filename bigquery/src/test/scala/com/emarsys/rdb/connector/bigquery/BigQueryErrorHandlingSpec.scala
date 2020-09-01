package com.emarsys.rdb.connector.bigquery

import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName}
import org.scalatest.PartialFunctionValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.TimeoutException

class BigQueryErrorHandlingSpec extends AnyWordSpecLike with Matchers with PartialFunctionValues {

  "BigQueryErrorHandling" should {
    "convert timeout exception to QueryTimeout" in new BigQueryErrorHandling {
      val exception = new TimeoutException("msg")
      val expected  = DatabaseError(ErrorCategory.Timeout, ErrorName.QueryTimeout, exception)

      eitherErrorHandler.valueAt(exception) shouldBe Left(expected)
    }

    "not convert DatabaseErrors" in new BigQueryErrorHandling {
      val error = DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SqlSyntaxError, "error", None, None)

      eitherErrorHandler.valueAt(error) shouldBe Left(error)
    }

    "convert every other exception into unknown DatabaseError" in new BigQueryErrorHandling {
      val exception = new RuntimeException("Explosion")
      val error     = DatabaseError(ErrorCategory.Unknown, ErrorName.Unknown, exception)

      eitherErrorHandler.valueAt(exception) shouldBe Left(error)
    }
  }
}
