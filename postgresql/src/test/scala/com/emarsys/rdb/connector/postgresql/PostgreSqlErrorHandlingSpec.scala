package com.emarsys.rdb.connector.postgresql

import java.sql.{SQLException, SQLTransientConnectionException}

import com.emarsys.rdb.connector.common.models.Errors._
import org.postgresql.util.{PSQLException, PSQLState}
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{EitherValues, Matchers, PartialFunctionValues, WordSpec}
import slick.SlickException

class PostgreSqlErrorHandlingSpec
    extends WordSpec
    with Matchers
    with TableDrivenPropertyChecks
    with EitherValues
    with PartialFunctionValues {

  private val slickExceptionWrongUpdate          = new SlickException("Update statements should not return a ResultSet")
  private val slickExceptionUnknown              = new SlickException("Unknown")
  private val queryTimeoutException              = new SQLException("msg", "57014")
  private val syntaxErrorException               = new PSQLException("msg", PSQLState.SYNTAX_ERROR)
  private val columnNotFoundException            = new SQLException("msg", "42703")
  private val invalidTextRepresentationException = new SQLException("msg", "22P02")
  private val permissionDeniedException          = new SQLException("msg", "42501")
  private val tableNotFoundException             = new SQLException("msg", "42P01")
  private val unableToConnectException           = new PSQLException("", PSQLState.CONNECTION_UNABLE_TO_CONNECT)
  private val invalidAuthorizationException      = new PSQLException("msg", PSQLState.INVALID_AUTHORIZATION_SPECIFICATION)
  private val connectionFailureException         = new PSQLException("msg", PSQLState.CONNECTION_FAILURE)
  private val invalidPasswordException           = new SQLException("msg", "28P01")
  private val timeoutMessage                     = "Connection is not available, request timed out after 5000ms."
  private val connectionTimeoutException         = new SQLTransientConnectionException(timeoutMessage)

  private val testCases = Table(
    ("database error", "sqlException", "clientError"),
    (
      "SlickException with wrong update statement",
      slickExceptionWrongUpdate,
      SqlSyntaxError("Wrong update statement: non update query given")
    ),
    ("unknown SlickExceptions", slickExceptionUnknown, ErrorWithMessage(slickExceptionUnknown.getMessage)),
    ("query timeout", queryTimeoutException, QueryTimeout(queryTimeoutException.getMessage)),
    ("syntax error", syntaxErrorException, SqlSyntaxError(syntaxErrorException.getMessage)),
    ("column not found error", columnNotFoundException, SqlSyntaxError(columnNotFoundException.getMessage)),
    (
      "invalid text representation error",
      invalidTextRepresentationException,
      SqlSyntaxError(invalidTextRepresentationException.getMessage)
    ),
    ("permission denied error", permissionDeniedException, AccessDeniedError(permissionDeniedException.getMessage)),
    ("table not found error", tableNotFoundException, TableNotFound(tableNotFoundException.getMessage)),
    ("unable to connect error", unableToConnectException, ConnectionError(unableToConnectException)),
    ("invalid authorization error", invalidAuthorizationException, ConnectionError(invalidAuthorizationException)),
    ("connection failure error", connectionFailureException, ConnectionError(connectionFailureException)),
    ("invalid password error", invalidPasswordException, ConnectionError(invalidPasswordException)),
    ("connection times out", connectionTimeoutException, ConnectionTimeout(timeoutMessage))
  )

  "PostgreSqlErrorHandling" should {

    forAll(testCases) {
      case (errorType, sqlException, connectorError) =>
        s"convert $errorType to ${connectorError.getClass.getSimpleName}" in new PostgreSqlErrorHandling {
          val actualError = eitherErrorHandler().valueAt(sqlException)

          actualError.left.value shouldBe connectorError
          actualError.left.value.getCause shouldBe sqlException
        }
    }
  }
}
