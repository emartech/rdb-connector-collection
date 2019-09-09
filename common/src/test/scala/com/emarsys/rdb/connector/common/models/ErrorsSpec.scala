package com.emarsys.rdb.connector.common.models

import org.scalatest.{Matchers, WordSpecLike}
import com.emarsys.rdb.connector.common.models.Errors.ErrorCategory.Timeout
import com.emarsys.rdb.connector.common.models.Errors.{ErrorName, ErrorPayload}

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
}
