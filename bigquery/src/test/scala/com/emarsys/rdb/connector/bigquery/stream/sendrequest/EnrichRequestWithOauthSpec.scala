package com.emarsys.rdb.connector.bigquery.stream.sendrequest

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import akka.util.Timeout
import com.emarsys.rdb.connector.bigquery.GoogleSession
import com.emarsys.rdb.connector.bigquery.stream.sendrequest.EnrichRequestWithOauth.TokenErrorException
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class EnrichRequestWithOauthSpec
    extends TestKit(ActorSystem("EnrichRequestWithOauthSpec"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with MockitoSugar {

  override def afterAll() = {
    shutdown()
  }

  implicit val timeout: Timeout = Timeout(1.second)

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
