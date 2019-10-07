package com.emarsys.rdb.connector.bigquery.stream.sendrequest

import akka.actor.ActorSystem
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model.StatusCodes.{BadRequest, Forbidden, NotFound}
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import akka.util.Timeout
import com.emarsys.rdb.connector.bigquery.GoogleSession
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory => C, ErrorName => N}
import com.emarsys.rdb.connector.test.CustomMatchers._
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class SendRequestWithOauthHandlingSpec
    extends TestKit(ActorSystem("SendRequestWithOauthHandlingSpec"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with MockitoSugar
    with TableDrivenPropertyChecks {
  import system.dispatcher

  override def afterAll: Unit = {
    shutdown()
  }

  implicit val materializer = ActorMaterializer()
  implicit val timeout      = Timeout(1.second)

  val errorCases = Table(
    ("error", "responseStatus", "message", "errorCategory", "errorName"),
    ("SyntaxError", BadRequest, "Syntax error", C.FatalQueryExecution, N.SqlSyntaxError),
    ("SyntaxError", BadRequest, "invalidQuery", C.FatalQueryExecution, N.SqlSyntaxError),
    ("NotFoundTable", NotFound, "Not found: Table", C.FatalQueryExecution, N.TableNotFound),
    ("NotFoundDataset", NotFound, "Not found: Dataset", C.FatalQueryExecution, N.TableNotFound),
    ("NotFoundProject", BadRequest, "The project xxx has not enabled", C.FatalQueryExecution, N.TableNotFound),
    ("RateLimit", Forbidden, "rateLimitExceeded, Exceeded rate limits", C.RateLimit, N.TooManyQueries)
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
