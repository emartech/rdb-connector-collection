package com.emarsys.rdb.connector.mssql

import java.sql.{SQLException, SQLTransientConnectionException}

import com.emarsys.rdb.connector.common.models.Errors._
import com.microsoft.sqlserver.jdbc.SQLServerException
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, PrivateMethodTester, WordSpecLike}

class MsSqlErrorHandlingSpec extends WordSpecLike with Matchers with MockitoSugar with PrivateMethodTester {

  val possibleSQLErrorsCodes = Seq(
    ("HY008", "query cancelled", QueryTimeout("msg")),
    ("S0001", "sql syntax error", SqlSyntaxError("msg")),
    ("S0005", "permission denied", AccessDeniedError("msg")),
    ("S0002", "invalid object name", TableNotFound("msg")),
    ("23000", "duplicate key in object", SqlSyntaxError("msg"))
  )

  val possibleConnectionErrors = Seq(
    ("08S01", "bad host error")
  )

  private def shouldBeWithCause[T](
      result: Either[ConnectorError, T],
      expected: ConnectorError,
      expectedCause: Throwable
  ): Unit = {
    result shouldBe Left(expected)
    result.left.get.getCause shouldBe expectedCause
  }

  "ErrorHandling" should {

    possibleSQLErrorsCodes.foreach(
      errorWithResponse =>
        s"""convert ${errorWithResponse._2} to ${errorWithResponse._3.getClass.getSimpleName}""" in new MsSqlErrorHandling {
          val error = new SQLServerException("msg", errorWithResponse._1, 0, new Exception())
          shouldBeWithCause(eitherErrorHandler().apply(error), errorWithResponse._3, error)
        }
    )

    possibleConnectionErrors.foreach(
      errorCode =>
        s"""convert ${errorCode._2} to ConnectionError""" in new MsSqlErrorHandling {
          val error = new SQLException("msg", errorCode._1)
          shouldBeWithCause(eitherErrorHandler().apply(error), ConnectionError(error), error)
        }
    )

    "handle EXPLAIN/SHOW can not be issued" in new MsSqlErrorHandling {
      val msg   = "EXPLAIN/SHOW can not be issued; lacking privileges for underlying table"
      val error = new SQLException(msg, "HY000")
      shouldBeWithCause(eitherErrorHandler().apply(error), AccessDeniedError(msg), error)
    }

    "convert timeout transient sql error to connection timeout error" in new MsSqlErrorHandling {
      val msg   = "Connection is not available, request timed out after"
      val error = new SQLTransientConnectionException(msg)
      shouldBeWithCause(eitherErrorHandler().apply(error), ConnectionTimeout(msg), error)
    }

    "convert sql error to error with message and state if not timeout" in new MsSqlErrorHandling {
      val msg   = "Other transient error"
      val error = new SQLTransientConnectionException(msg, "not-handled-sql-state", 999)
      shouldBeWithCause(
        eitherErrorHandler().apply(error),
        ErrorWithMessage(s"[not-handled-sql-state] - [999] - $msg"),
        error
      )
    }

  }
}
