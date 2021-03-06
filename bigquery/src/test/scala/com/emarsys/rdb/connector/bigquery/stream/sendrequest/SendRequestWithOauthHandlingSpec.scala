package com.emarsys.rdb.connector.bigquery.stream.sendrequest

import akka.actor.ActorSystem
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model.StatusCodes.{BadRequest, Forbidden, NotFound}
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import akka.util.Timeout
import com.emarsys.rdb.connector.bigquery.GoogleSession
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory => C, ErrorName => N}
import com.emarsys.rdb.connector.test.CustomMatchers._
import org.mockito.Mockito._
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class SendRequestWithOauthHandlingSpec
    extends TestKit(ActorSystem("SendRequestWithOauthHandlingSpec"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with MockitoSugar
    with TableDrivenPropertyChecks {
  import system.dispatcher

  override def afterAll(): Unit = {
    shutdown()
  }

  implicit val timeout: Timeout = Timeout(1.second)

  val quotaExceededErrorBody = "Quota exceeded: Your project exceeded quota for free query bytes scanned."

  val errorCases = Table(
    ("error", "responseStatus", "message", "errorCategory", "errorName"),
    ("SyntaxError", BadRequest, "Syntax error", C.FatalQueryExecution, N.SqlSyntaxError),
    ("SyntaxError", BadRequest, "invalidQuery", C.FatalQueryExecution, N.SqlSyntaxError),
    ("NotFoundTable", NotFound, "Not found: Table", C.FatalQueryExecution, N.TableNotFound),
    ("NotFoundDataset", NotFound, "Not found: Dataset", C.FatalQueryExecution, N.TableNotFound),
    ("NotFoundProject", BadRequest, "The project xxx has not enabled", C.FatalQueryExecution, N.TableNotFound),
    ("QuotaExceeded", Forbidden, quotaExceededErrorBody, C.FatalQueryExecution, N.QueryRejected),
    ("RateLimit", Forbidden, "rateLimitExceeded, Exceeded rate limits", C.RateLimit, N.TooManyQueries),
    (
      "AccessDeniedError",
      Forbidden,
      "Access Denied: Project my-project: User does not have abc123 permission in project my-project.",
      C.FatalQueryExecution,
      N.AccessDeniedError
    )
  )

  trait SendRequestScope {
    def response: HttpResponse

    val request = HttpRequest().addHeader(Authorization(OAuth2BearerToken("TOKEN")))
    val session = mock[GoogleSession]
    val http    = mock[HttpExt]

    lazy val resultF = {
      when(session.getToken()) thenReturn Future.successful("TOKEN")
      when(http.singleRequest(request)) thenReturn Future.successful(response)

      Source
        .single(HttpRequest())
        .via(SendRequestWithOauthHandling(session, http))
        .runWith(Sink.last)
    }

    def getResult: HttpResponse = Await.result(resultF, 1.second)
  }

  "SendRequestWithOauthHandling" when {
    forAll(errorCases) {
      case (_, responseStatus, message, errorCategory, errorName) =>
        s"$message is in the response body with status code $responseStatus" should {
          s"result in ${errorCategory}#$errorName" in new SendRequestScope {
            val responseBody =
              s"""
                 |{
                 | "error": {
                 |  "message": "$message"
                 | }
                 |}
          """.stripMargin

            override def response = HttpResponse(responseStatus, Nil, HttpEntity(responseBody))

            the[DatabaseError] thrownBy getResult should haveErrorCategoryAndErrorName(errorCategory, errorName)
          }
        }
    }

    "convert unexpected http error to unknown DatabaseError" in new SendRequestScope {
      val body              = "my custom error"
      override def response = HttpResponse(StatusCodes.InternalServerError, Nil, HttpEntity(body))
      val message           = s"Unexpected error in response: 500 Internal Server Error, $body"
      val expectedError     = DatabaseError(C.Unknown, N.Unknown, message, None, None)
      val actualError       = the[DatabaseError] thrownBy getResult

      actualError should beDatabaseErrorEqualWithoutCause(expectedError)
    }
  }
}
