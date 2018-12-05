package com.emarsys.rdb.connector.redshift

import java.sql.SQLException
import java.util.concurrent.RejectedExecutionException

import com.emarsys.rdb.connector.common.models.Errors._
import org.scalatest.{Matchers, WordSpecLike}

class RedshiftErrorHandlingSpec extends WordSpecLike with Matchers {

  "MySqlErrorHandling" should {

    val possibleSQLErrors = Seq(
      (null, "Connection is not available, request timed out after", ConnectionTimeout("Connection is not available, request timed out after")),
      ("HY000", "connection timeout", ConnectionTimeout("connection timeout")),
      ("57014", "query cancelled", QueryTimeout("query cancelled")),
      ("42601", "sql syntax error", SqlSyntaxError("sql syntax error")),
      ("42501", "permission denied", AccessDeniedError("permission denied")),
      ("42P01", "relation not found", TableNotFound("relation not found"))
    )

    val possibleConnectionErrors = Seq(
      ("08001", "unable to connect"),
      ("28000", "invalid authorization"),
      ("08006", "server process is terminating"),
      ("28P01", "invalid password")
    )

    possibleSQLErrors.foreach {
      case (sqlState, message, error) =>
        s"""convert $message to ${error.getClass.getSimpleName}""" in new RedshiftErrorHandling {
          val e = new SQLException(message, sqlState)
          eitherErrorHandler.apply(e) shouldEqual Left(error)
        }
    }

    possibleConnectionErrors.foreach { case (sqlState, message) =>
      s"""convert $message to ConnectionError""" in new RedshiftErrorHandling {
        val e = new SQLException("msg", sqlState)
        eitherErrorHandler.apply(e) shouldEqual Left(ConnectionError(e))
      }
    }

    "convert RejectedExecutionException to TooManyQueries" in new RedshiftErrorHandling {
      val e = new RejectedExecutionException("msg")
      eitherErrorHandler.apply(e) shouldEqual Left(TooManyQueries("msg"))
    }

    "convert unhandled SQLException to ErrorWithMessage" in new RedshiftErrorHandling {
      val e = new SQLException("msg", "state")
      eitherErrorHandler.apply(e) shouldEqual Left(ErrorWithMessage("[state] msg"))
    }

    "convert unknown exception to ErrorWithMessage" in new RedshiftErrorHandling {
      val e = new Exception("msg")
      eitherErrorHandler.apply(e) shouldEqual Left(ErrorWithMessage("msg"))
    }
  }
}
