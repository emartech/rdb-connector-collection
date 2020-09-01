package com.emarsys.rdb.connector.common.models

import com.emarsys.rdb.connector.common.models.Errors._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class ErrorsSpec extends AnyWordSpecLike with Matchers {
  "ErrorNames" should {
    "have a nice unapply for strings" in {
      val results = for {
        error <- ErrorName.values
        selfIsMatched = error.unapply(error.entryName)
        othersAreNot  = ErrorName.values.filterNot(_ == error).forall(_.unapply(error.entryName) == false)
      } yield selfIsMatched && othersAreNot

      results should contain only true
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
