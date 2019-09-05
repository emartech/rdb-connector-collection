package com.emarsys.rdb.connector.test

import com.emarsys.rdb.connector.common.models.Errors.{ConnectorError, DatabaseError}
import org.scalatest.matchers.{MatchResult, Matcher}

object CustomMatchers extends CustomMatchers
trait CustomMatchers {

  class DatabaseErrorMatcher(expected: DatabaseError) extends Matcher[ConnectorError] {
    override def apply(actual: ConnectorError): MatchResult =
      actual match {
        case DatabaseError(errorCategory, error, message, _, context) =>
          MatchResult(
            errorCategory == expected.errorCategory && error == expected.error && message
              .contains(expected.message) && context == expected.context,
            s"\nActual DatabaseError: $errorCategory, $error, $message, $context\nExpected DatabaseError: ${expected.errorCategory}, ${expected.error}, ${expected.message}, ${expected.context}\n",
            "Actual and expected DatabaseErrors are equal (except for cause, it is not checked for equality)"
          )
        case ee =>
          MatchResult(matches = false, s"Actual error is not a DatabaseError. It is $ee.", "This.Should.Never.Happen.")
      }
  }

  def beDatabaseErrorEqualWithoutCause(expected: DatabaseError) = new DatabaseErrorMatcher(expected)
}
