package com.emarsys.rdb.connector.bigquery.stream.sendrequest

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.{TestKit, TestProbe}
import akka.util.Timeout
import com.emarsys.rdb.connector.bigquery.GoogleTokenActor.{TokenError, TokenRequest, TokenResponse}
import com.emarsys.rdb.connector.bigquery.stream.sendrequest.EnrichRequestWithOauth.TokenErrorException
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._

class EnrichRequestWithOauthSpec extends TestKit(ActorSystem("EnrichRequestWithOauthSpec"))
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  implicit val materializer = ActorMaterializer()
  implicit val timeout = Timeout(1.second)

  import system.dispatcher

  "EnrichRequestWithOauth" must {

    "Ask token actor, add oauth header" in {
      val testTokenActor = TestProbe()

      val resultF = Source.single((HttpRequest(), false))
        .via(EnrichRequestWithOauth(testTokenActor.testActor))
        .runWith(Sink.last)

      testTokenActor.expectMsg(TokenRequest(false))
      testTokenActor.reply(TokenResponse("TOKEN"))

      val result = Await.result(resultF, 1.second)
      result.headers.head.name() shouldEqual "Authorization"
      result.headers.head.value() shouldEqual "Bearer TOKEN"
    }

    "Ask token actor with force, add oauth header" in {
      val testTokenActor = TestProbe()

      val resultF = Source.single((HttpRequest(), true))
        .via(EnrichRequestWithOauth(testTokenActor.testActor))
        .runWith(Sink.last)

      testTokenActor.expectMsg(TokenRequest(true))
      testTokenActor.reply(TokenResponse("TOKEN"))

      val result = Await.result(resultF, 1.second)
      result.headers.head.name() shouldEqual "Authorization"
      result.headers.head.value() shouldEqual "Bearer TOKEN"
    }

    "Token actor response error" in {
      val testTokenActor = TestProbe()

      val resultF = Source.single((HttpRequest(), true))
        .via(EnrichRequestWithOauth(testTokenActor.testActor))
        .runWith(Sink.last)

      testTokenActor.expectMsg(TokenRequest(true))
      testTokenActor.reply(TokenError)

      assertThrows[TokenErrorException.type] {
        Await.result(resultF, 1.second)
      }
    }

  }
}
