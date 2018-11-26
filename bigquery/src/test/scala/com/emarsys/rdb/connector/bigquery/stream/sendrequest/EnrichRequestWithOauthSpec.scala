package com.emarsys.rdb.connector.bigquery.stream.sendrequest

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import akka.util.Timeout
import com.emarsys.rdb.connector.bigquery.GoogleSession
import com.emarsys.rdb.connector.bigquery.stream.sendrequest.EnrichRequestWithOauth.TokenErrorException
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class EnrichRequestWithOauthSpec
    extends TestKit(ActorSystem("EnrichRequestWithOauthSpec"))
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

  "EnrichRequestWithOauth" must {

    "Ask token actor, add oauth header" in {
      val session = mock[GoogleSession]
      when(session.getToken()) thenReturn Future.successful("TOKEN")
      val resultF = Source
        .single(HttpRequest())
        .via(EnrichRequestWithOauth(session))
        .runWith(Sink.last)

      val result = Await.result(resultF, 1.second)
      result.headers.head.name() shouldEqual "Authorization"
      result.headers.head.value() shouldEqual "Bearer TOKEN"
    }

    "Token actor response error" in {
      val session = mock[GoogleSession]
      when(session.getToken()) thenReturn Future.failed(TokenErrorException())

      val resultF = Source
        .single(HttpRequest())
        .via(EnrichRequestWithOauth(session))
        .runWith(Sink.last)

      assertThrows[TokenErrorException] {
        Await.result(resultF, 1.second)
      }
    }

  }
}
