package com.emarsys.rdb.connector.redshift

import java.sql.{SQLException, SQLTransientConnectionException}

import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName}
import com.emarsys.rdb.connector.test.CustomMatchers._
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class RedshiftErrorHandlingSpec extends AnyWordSpecLike with Matchers with EitherValues {

  val possibleSQLErrors: Seq[(String, String, DatabaseError)] = Seq(
    (
      "HY000",
      "Connection is not available, request timed out after",
      DatabaseError(ErrorCategory.Timeout, ErrorName.ConnectionTimeout, "", None, None)
    ),
    (
      "HY000",
      "[Amazon](500053) The TCP Socket has timed out while waiting for response",
      DatabaseError(ErrorCategory.Timeout, ErrorName.QueryTimeout, "", None, None)
    ),
    ("HY000", "other error with HY000", DatabaseError(ErrorCategory.Unknown, ErrorName.Unknown, "", None, None)),
    ("57014", "query cancelled", DatabaseError(ErrorCategory.Timeout, ErrorName.QueryTimeout, "", None, None)),
    (
      "42601",
      "sql syntax error",
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SqlSyntaxError, "", None, None)
    ),
    (
      "42501",
      "permission denied",
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.AccessDeniedError, "", None, None)
    ),
    (
      "42P01",
      "relation not found",
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.TableNotFound, "", None, None)
    ),
    (
      "42702",
      "[Amazon](500310) Invalid operation: column reference \"seller_id\" is ambiguous;",
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SqlSyntaxError, "", None, None)
    ),
    (
      "22P02",
      "[Amazon](500310) Invalid operation: invalid input syntax for integer: \"2019-07-10\";",
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SqlSyntaxError, "", None, None)
    ),
    (
      "42703",
      "[Amazon](500310) Invalid operation: column t.customer_email does not exist;",
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SqlSyntaxError, "", None, None)
    ),
    (
      "42883",
      "[Amazon](500310) Invalid operation: function whatever does not exist;",
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SqlSyntaxError, "", None, None)
    ),
    (
      "42P02",
      "[Amazon](500310) Invalid operation: parameter whatever does not exist;",
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SqlSyntaxError, "", None, None)
    ),
    (
      "42704",
      "[Amazon](500310) Invalid operation: object whatever does not exist;",
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SqlSyntaxError, "", None, None)
    )
  )

  val possibleConnectionErrors = Seq(
    ("08001", "unable to connect"),
    ("28000", "invalid authorization"),
    ("08006", "server process is terminating"),
    ("28P01", "invalid password")
  )

  "RedshiftErrorHandling" should {

    possibleSQLErrors.foreach {
      case (sqlState, message, expectedError) =>
        s"""convert $message to ${expectedError.getClass.getSimpleName}""" in new RedshiftErrorHandling {
          val thrownError = new SQLException(message, sqlState, 999)
          val result      = eitherErrorHandler.apply(thrownError)
          result.left.value should beDatabaseErrorEqualWithoutCause(expectedError)
          result.left.value.cause shouldBe Some(thrownError)
        }
    }

    possibleConnectionErrors.foreach {
      case (sqlState, message) =>
        s"""convert $message to ConnectionError""" in new RedshiftErrorHandling {
          val thrownError   = new SQLException("msg", sqlState)
          val result        = eitherErrorHandler.apply(thrownError)
          val expectedError = DatabaseError(ErrorCategory.Unknown, ErrorName.Unknown, "", None, None)
          result.left.value should beDatabaseErrorEqualWithoutCause(expectedError)
          result.left.value.cause shouldBe Some(thrownError)
        }
    }

    "convert timeout transient sql error to connection timeout error" in new RedshiftErrorHandling {
      val msg         = "Connection is not available, request timed out after"
      val thrownError = new SQLTransientConnectionException(msg)
      val result      = eitherErrorHandler.apply(thrownError)
      result.left.value should haveErrorCategoryAndErrorName(ErrorCategory.Timeout, ErrorName.ConnectionTimeout)
      result.left.value.cause shouldBe Some(thrownError)
    }

    "convert sql error to error with message and state if not timeout" in new RedshiftErrorHandling {
      val msg         = "Other transient error"
      val thrownError = new SQLTransientConnectionException(msg, "not-handled-sql-state", 999)
      val result      = eitherErrorHandler.apply(thrownError)
      result.left.value should haveErrorCategoryAndErrorName(ErrorCategory.Unknown, ErrorName.Unknown)
      result.left.value.cause shouldBe Some(thrownError)
    }
  }
}
