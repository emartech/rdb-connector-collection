package com.emarsys.rdb.connector.test

import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName}
import org.scalatest.matchers.{Matcher, MatchResult}

object CustomMatchers extends CustomMatchers
trait CustomMatchers {

  class DatabaseErrorMatcher(expected: DatabaseError) extends Matcher[DatabaseError] {
    override def apply(actual: DatabaseError): MatchResult =
      actual match {
        case DatabaseError(errorCategory, error, message, _, context) =>
          MatchResult(
            errorCategory == expected.errorCategory && error == expected.errorName && message
              .contains(expected.message) && context == expected.context,
            s"\nActual DatabaseError: $errorCategory, $error, $message, $context\nExpected DatabaseError: ${expected.errorCategory}, ${expected.errorName}, ${expected.message}, ${expected.context}\n",
            "Actual and expected DatabaseErrors are equal (except for cause, it is not checked for equality)"
          )
        case ee =>
          MatchResult(matches = false, s"Actual error is not a DatabaseError. It is $ee.", "This.Should.Never.Happen.")
      }
  }

  def beDatabaseErrorEqualWithoutCause(expected: DatabaseError) = new DatabaseErrorMatcher(expected)

  class ErrorCategoryAndErrorNameMatcher(errorCategory: ErrorCategory, errorName: ErrorName)
      extends Matcher[DatabaseError] {
    override def apply(actual: DatabaseError): MatchResult =
      actual match {
        case DatabaseError(actualErrorCategory, actualErrorName, _, _, _) =>
          MatchResult(
            actualErrorCategory == errorCategory && actualErrorName == errorName,
            s"\nActual: $actualErrorCategory, $actualErrorName\nExpected: $errorCategory, $errorName",
            "Actual and expected error categories and error names are equal"
          )
        case ee =>
          MatchResult(matches = false, s"$ee did not have $errorCategory and $errorName", "This.Should.Never.Happen.")
      }
  }

  def haveErrorCategoryAndErrorName(errorCategory: ErrorCategory, errorName: ErrorName) =
    new ErrorCategoryAndErrorNameMatcher(errorCategory, errorName)
}
