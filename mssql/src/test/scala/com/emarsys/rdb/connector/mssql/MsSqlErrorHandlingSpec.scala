package com.emarsys.rdb.connector.mssql

import java.sql.{SQLException, SQLTransientConnectionException}

import com.emarsys.rdb.connector.common.models.Errors._
import com.microsoft.sqlserver.jdbc.SQLServerException
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.{EitherValues, Matchers, PrivateMethodTester, WordSpecLike}
import com.emarsys.rdb.connector.test.CustomMatchers._

class MsSqlErrorHandlingSpec
    extends WordSpecLike
    with Matchers
    with MockitoSugar
    with PrivateMethodTester
    with EitherValues {

  case class ErrorCase(
      errorCaseName: String,
      errorCode: String,
      errorMessage: String,
      expectedError: DatabaseError
  )

  val possibleErrors = Seq(
    (
      "query cancelled",
      new SQLServerException("Error1", "HY008", 0, new Exception()),
      DatabaseError(ErrorCategory.Timeout, ErrorName.QueryTimeout, new Exception(""))
    ),
    (
      "sql syntax error",
      new SQLServerException("Error1", "S0001", 0, new Exception()),
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SqlSyntaxError, new Exception(""))
    ),
    (
      "duplicate key in object",
      new SQLServerException("Error1", "23000", 0, new Exception()),
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SqlSyntaxError, new Exception(""))
    ),
    (
      "invalid object name",
      new SQLServerException("Error1", "S0002", 0, new Exception()),
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.TableNotFound, new Exception(""))
    ),
    (
      "permission denied",
      new SQLServerException("Error1", "S0005", 0, new Exception()),
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.AccessDeniedError, new Exception(""))
    ),
    (
      "showplan permission denied",
      new SQLServerException("Error1", "S0004", 0, new Exception()),
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.AccessDeniedError, new Exception(""))
    ),
    (
      "explain permission denied",
      new SQLException(
        "EXPLAIN/SHOW can not be issued; lacking privileges for underlying table",
        "HY000",
        0,
        new Exception()
      ),
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.AccessDeniedError, new Exception(""))
    ),
    (
      "timeout transient sql error",
      new SQLTransientConnectionException("Connection is not available, request timed out after"),
      DatabaseError(ErrorCategory.Timeout, ErrorName.ConnectionTimeout, new Exception(""))
    ),
    (
      "unknown error",
      new SQLTransientConnectionException("Other transient error", "not-handled-sql-state", 999),
      DatabaseError(
        ErrorCategory.Unknown,
        ErrorName.Unknown,
        s"[not-handled-sql-state] - [999] - Other transient error"
      )
    )
  )

  "ErrorHandling" should {

    possibleErrors.foreach {
      case (errorCaseName, thrownError, expectedError) =>
        s"convert $errorCaseName to ${expectedError.getClass.getSimpleName}" in new MsSqlErrorHandling {
          val result = eitherErrorHandler().apply(thrownError)
          result.left.value should beDatabaseErrorEqualWithoutCause(expectedError)
          result.left.get.asInstanceOf[DatabaseError].cause shouldBe Some(thrownError)
        }
    }

  }
}
