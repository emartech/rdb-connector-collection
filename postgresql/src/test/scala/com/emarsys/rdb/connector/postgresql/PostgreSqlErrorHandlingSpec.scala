package com.emarsys.rdb.connector.postgresql

import java.util.concurrent.RejectedExecutionException

import com.emarsys.rdb.connector.common.models.Errors.{
  ConnectionError,
  ErrorWithMessage,
  SqlSyntaxError,
  TooManyQueries
}
import org.postgresql.util.{PSQLException, PSQLState}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, PrivateMethodTester, WordSpecLike}

class PostgreSqlErrorHandlingSpec extends WordSpecLike with Matchers with MockitoSugar with PrivateMethodTester {

  "PostgreSqlErrorHandling" should {

    val unableToConnectException      = new PSQLException("", PSQLState.CONNECTION_UNABLE_TO_CONNECT)
    val invalidAuthorizationException = new PSQLException("msg", PSQLState.INVALID_AUTHORIZATION_SPECIFICATION)
    val connectionFailureException    = new PSQLException("msg", PSQLState.CONNECTION_FAILURE)

    Seq(
      ("syntax error", "SqlSyntaxError", new PSQLException("msg", PSQLState.SYNTAX_ERROR), SqlSyntaxError("msg")),
      (
        "unable to connect error",
        "ConnectionError",
        unableToConnectException,
        ConnectionError(unableToConnectException)
      ),
      (
        "invalid authorization error",
        "ConnectionError",
        invalidAuthorizationException,
        ConnectionError(invalidAuthorizationException)
      ),
      (
        "connection failure error",
        "ConnectionError",
        connectionFailureException,
        ConnectionError(connectionFailureException)
      ),
      ("RejectedExecutionException", "TooManyQueries", new RejectedExecutionException("msg"), TooManyQueries("msg")),
      ("unidentified exception", "ErrorWithMessage", new Exception("msg"), ErrorWithMessage("msg"))
    ).foreach { test =>
      s"convert ${test._1} to ${test._2}" in new PostgreSqlErrorHandling {
        eitherErrorHandler.apply(test._3) shouldEqual Left(test._4)
      }
    }
  }
}
