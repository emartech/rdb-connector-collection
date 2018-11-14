package com.emarsys.rdb.connector.redshift

import java.sql.SQLException
import java.util.concurrent.RejectedExecutionException

import com.emarsys.rdb.connector.common.models.Errors._
import org.scalatest.{Matchers, WordSpecLike}

class RedshiftErrorHandlingSpec extends WordSpecLike with Matchers {

  "MySqlErrorHandling" should {

    val possibleSQLErrorsCodes = Seq(
      ("HY000", "connection timeout", ConnectionTimeout("msg")),
      ("57014", "query cancelled", QueryTimeout("msg")),
      ("42601", "sql syntax error", SqlSyntaxError("msg")),
      ("42501", "permission denied", AccessDeniedError("msg")),
      ("42P01", "relation not found", TableNotFound("msg"))
    )

    val possibleConnectionErrors = Seq(
      ("08001", "unable to connect"),
      ("28000", "invalid authorization"),
      ("08006", "server process is terminating"),
      ("28P01", "invalid password")
    )

    possibleSQLErrorsCodes.foreach(
      errorWithResponse =>
        s"""convert ${errorWithResponse._2} to ${errorWithResponse._3.getClass.getSimpleName}""" in new RedshiftErrorHandling {
          val e = new SQLException("msg", errorWithResponse._1)
          eitherErrorHandler.apply(e) shouldEqual Left(errorWithResponse._3)
        }
    )

    possibleConnectionErrors.foreach(
      errorCode =>
        s"""convert ${errorCode._2} to ConnectionError""" in new RedshiftErrorHandling {
          val e = new SQLException("msg", errorCode._1)
          eitherErrorHandler.apply(e) shouldEqual Left(ConnectionError(e))
        }
    )

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
