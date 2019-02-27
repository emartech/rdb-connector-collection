package com.emarsys.rdb.connector.bigquery.stream.sendrequest

import akka.actor.ActorSystem
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import akka.util.Timeout
import com.emarsys.rdb.connector.bigquery.GoogleSession
import com.emarsys.rdb.connector.common.models.Errors.{ErrorWithMessage, SqlSyntaxError, TooManyQueries}
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Try}

class SendRequestWithOauthHandlingSpec
    extends TestKit(ActorSystem("SendRequestWithOauthHandling"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with MockitoSugar {

  override def afterAll = {
    TestKit.shutdownActorSystem(system)
  }

  implicit val materializer = ActorMaterializer()
  implicit val timeout      = Timeout(1.second)

  import system.dispatcher

  trait SendRequestScope {
    def response: HttpResponse

    val session = mock[GoogleSession]
    val http = mock[HttpExt]

    lazy val resultF = {
      when(session.getToken()) thenReturn Future.successful("TOKEN")
      when(
        http.singleRequest(
          HttpRequest()
            .addHeader(Authorization(OAuth2BearerToken("TOKEN")))
        )
      ) thenReturn Future.successful(response)

      Source
        .single(HttpRequest())
        .via(SendRequestWithOauthHandling(session, http))
        .runWith(Sink.last)
    }

    lazy val result = Try(Await.result(resultF, 1.second))
  }

  "SendRequestWithOauthHandling" must {

    "convert unexpected http error to ErrorWithMessage" in new SendRequestScope {
      val response = HttpResponse(StatusCodes.InternalServerError, Nil, HttpEntity("my custom error"))

      result shouldEqual Failure(
        ErrorWithMessage("Unexpected error in response: 500 Internal Server Error, my custom error")
      )
    }

    "recognize syntax errors" when {
      "explicit syntax error is in the response body" in new SendRequestScope {
        val syntaxErrorMessage = "Syntax error, some detailed reason"
        val syntaxErrorResponseBody =
          s"""
            |{
            | "error": {
            |  "errors": [],
            |  "code": 400,
            |  "message": "$syntaxErrorMessage"
            | }
            |}
          """.stripMargin
        def response = HttpResponse(StatusCodes.BadRequest, Nil, HttpEntity(syntaxErrorResponseBody))

        result shouldEqual Failure(
          SqlSyntaxError(syntaxErrorMessage)
        )
      }

      "invalidQuery is in the response body" in new SendRequestScope {
        val invalidQueryMessage = "Unrecognized name: xxxxx; failed to parse view 'project.some_view' at [10:3]"
        val invalidQueryResponseBody =
          s"""
             |{
             | "error": {
             |  "errors": [
             |    {
             |      "domain": "global",
             |      "reason": "invalidQuery",
             |      "message": "$invalidQueryMessage",
             |      "locationType": "other",
             |      "location": "query"
             |   }
             |  ],
             |  "code": 400,
             |  "message": "$invalidQueryMessage"
             | }
             |}
          """.stripMargin
        def response = HttpResponse(StatusCodes.BadRequest, Nil, HttpEntity(invalidQueryResponseBody))

        result shouldEqual Failure(
          SqlSyntaxError(invalidQueryMessage)
        )
      }
    }

    "recognize rate limit errors" in new SendRequestScope {
      val rateLimitMessage = "Exceeded rate limits: Your project exceeded quota for concurrent queries. For more information, see https://cloud.google.com/bigquery/troubleshooting-errors"
      val rateLimitResponseBody =
        s"""
           |{
           | "error": {
           |  "errors": [
           |   {
           |    "domain": "global",
           |    "reason": "rateLimitExceeded",
           |    "message": "$rateLimitMessage",
           |    "locationType": "other",
           |    "location": "dummy_connection"
           |   }
           |  ],
           |  "code": 403,
           |  "message": "Exceeded rate limits: Your project exceeded quota for concurrent queries. For more information, see https://cloud.google.com/bigquery/troubleshooting-errors"
           | }
           |}
          """.stripMargin

      val response = HttpResponse(StatusCodes.Forbidden, Nil, HttpEntity(rateLimitResponseBody))
      result shouldEqual Failure(
        TooManyQueries(rateLimitMessage)
      )
    }
  }

}
