package com.emarsys.rdb.connector.common.models

import com.emarsys.rdb.connector.common.models.Errors.ErrorCategory.Timeout
import com.emarsys.rdb.connector.common.models.Errors._
import org.scalatest.{Matchers, WordSpecLike}

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

    "have a nice unapply for strings" in {
      val results = for {
        error <- ErrorName.values
        selfIsMatched = error.unapply(error.entryName)
        othersAreNot  = ErrorName.values.filterNot(_ == error).forall(_.unapply(error.entryName) == false)
      } yield selfIsMatched && othersAreNot

      results should contain only true
    }
  }

  "ErrorPayload" should {
    "return informative toString" in {
      val errorPayload = ErrorPayload(
        errorCategory = ErrorCategory.Timeout,
        errorName = ErrorName.QueryTimeout.toString,
        message = "msg1",
        causes = List(Cause("cause1"), Cause("cause2")),
        context = Some(Fields(List("field1", "field2")))
      )
      errorPayload.toString shouldBe "ErrorPayload(Timeout,QueryTimeout,msg1,List(Cause(cause1), Cause(cause2)),Some(Fields(List(field1, field2))))"
    }
  }

  "DatabaseError" should {
    "return informative toString" in {
      val databaseError = DatabaseError(
        errorCategory = ErrorCategory.Timeout,
        errorName = ErrorName.QueryTimeout,
        message = "msg1",
        cause = Some(new Exception("cause1", new Exception("cause2"))),
        context = Some(Fields(List("field1", "field2")))
      )
      databaseError.toString shouldBe "DatabaseError(Timeout,QueryTimeout,msg1,Some(java.lang.Exception: cause1),Some(Fields(List(field1, field2))))"
    }
  }
}
