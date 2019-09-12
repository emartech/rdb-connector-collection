package com.emarsys.rdb.connector.common.models

import org.scalatest.{Matchers, WordSpecLike}
import com.emarsys.rdb.connector.common.models.Errors.ErrorCategory.Timeout
import com.emarsys.rdb.connector.common.models.Errors.{Cause, DatabaseError, ErrorCategory, ErrorName, ErrorPayload}

class ErrorsSpec extends WordSpecLike with Matchers {
  "ErrorNames" should {
    "have a nice unapply for ErrorPayloads" in {
      val results = for {
        error <- ErrorName.values
        payload       = ErrorPayload(Timeout, error, "", None, None)
        selfIsMatched = error.unapply(payload)
        othersAreNot  = ErrorName.values.filterNot(_ == error).forall(_.unapply(payload) == false)
      } yield selfIsMatched && othersAreNot

      results should contain only true
    }
  }

  "ErrorPayload" should {
    "construct with proper causes with apply" in {
      val cause2 = new Exception("cause2")
      val cause1 = new Exception("cause1", cause2)
      val errorPayload = ErrorPayload(
        ErrorCategory.Timeout,
        ErrorName.QueryTimeout,
        "message1",
        cause = Some(cause1),
        context = None
      )
      errorPayload.causes shouldBe List(Cause("cause1"), Cause("cause2"))
    }

    "construct with proper causes with fromDatabaseError" in {
      val cause2 = new Exception("cause2")
      val cause1 = new Exception("cause1", cause2)
      val errorPayload = ErrorPayload.fromDatabaseError(
        DatabaseError(
          ErrorCategory.Timeout,
          ErrorName.QueryTimeout,
          "message1",
          cause = Some(cause1),
          context = None
        )
      )
      errorPayload.causes shouldBe List(Cause("cause1"), Cause("cause2"))
    }
  }
}
