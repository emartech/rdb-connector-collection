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
import com.emarsys.rdb.connector.common.models.Errors.ErrorWithMessage
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

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  implicit val materializer = ActorMaterializer()
  implicit val timeout      = Timeout(1.second)

  import system.dispatcher

  "SendRequestWithOauthHandling" must {

    "handle unexpected http error" in {

      val session = mock[GoogleSession]
      when(session.getToken()) thenReturn Future.successful("TOKEN")

      val http = mock[HttpExt]
      when(
        http.singleRequest(
          HttpRequest()
            .addHeader(Authorization(OAuth2BearerToken("TOKEN")))
        )
      ) thenReturn Future.successful(HttpResponse(StatusCodes.InternalServerError, Nil, HttpEntity("my custom error")))

      val resultF = Source
        .single(HttpRequest())
        .via(SendRequestWithOauthHandling(session, http))
        .runWith(Sink.last)

      val result = Try(Await.result(resultF, 1.second))
      result shouldEqual Failure(
        ErrorWithMessage("Unexpected error in response: 500 Internal Server Error, my custom error")
      )
    }

  }
}
