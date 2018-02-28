package com.emarsys.rdb.connector.bigquery.stream

import akka.NotUsed
import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.http.scaladsl.model._
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.http.scaladsl.{HttpExt, HttpsConnectionContext}
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, Graph, Materializer, SourceShape}
import akka.testkit.{TestKit, TestProbe}
import akka.util.Timeout
import cats.syntax.option._
import com.emarsys.rdb.connector.bigquery.GoogleTokenActor.{TokenRequest, TokenResponse}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.reflectiveCalls

class BigQueryStreamSourceSpec
    extends TestKit(ActorSystem("BigQueryStreamSourceSpec"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  implicit val timeout      = Timeout.durationToTimeout(3.seconds)
  implicit val materializer = ActorMaterializer()

  val mockHttp =
    new HttpExt(null) {
      var usedToken = ""
      var responseFn: HttpRequest => Future[HttpResponse] = { _ =>
        Future.successful(HttpResponse(entity = HttpEntity("{}")))
      }

      override def singleRequest(request: HttpRequest,
                                 connectionContext: HttpsConnectionContext,
                                 settings: ConnectionPoolSettings,
                                 log: LoggingAdapter)(implicit fm: Materializer): Future[HttpResponse] = {
        usedToken = request.headers.head.value().stripPrefix("Bearer ")
        responseFn(request)
      }
    }

  "BigQueryStreamSource" should {

    trait BigQuerySourceScope {
      val testTokenActorProbe = TestProbe()
    }

    "return single page request" in new BigQuerySourceScope {

      val bigQuerySource: Graph[SourceShape[String], NotUsed] =
        BigQueryStreamSource(HttpRequest(), _ => "success".some, testTokenActorProbe.ref, mockHttp)

      val resultF = Source.fromGraph(bigQuerySource).runWith(Sink.head)

      testTokenActorProbe.expectMsg(TokenRequest(false))
      testTokenActorProbe.reply(TokenResponse("TOKEN"))

      Await.result(resultF, 1.second) shouldBe "success"
      mockHttp.usedToken shouldBe "TOKEN"
    }

    "return two page request" in new BigQuerySourceScope {

      mockHttp.responseFn = { request =>
        request.uri.toString() match {
          case "/" =>
            Future.successful(
              HttpResponse(entity = HttpEntity("""{ "pageToken": "nextPage", "jobReference": { "jobId": "job123"} }"""))
            )
          case "/job123?pageToken=nextPage" =>
            Future.successful(HttpResponse(entity = HttpEntity("""{ }""")))
        }
      }
      val bigQuerySource = BigQueryStreamSource(HttpRequest(), _ => "success".some, testTokenActorProbe.ref, mockHttp)

      val resultF = bigQuerySource.runWith(Sink.seq)
      testTokenActorProbe.expectMsg(TokenRequest(false))
      testTokenActorProbe.reply(TokenResponse("TOKEN"))

      testTokenActorProbe.expectMsg(TokenRequest(false))
      testTokenActorProbe.reply(TokenResponse("TOKEN"))

      Await.result(resultF, 1.second) shouldBe Seq("success", "success")
      mockHttp.usedToken shouldBe "TOKEN"
    }

    "retry on failed auth" in new BigQuerySourceScope {

      val bigQuerySource = BigQueryStreamSource(HttpRequest(), _ => "success".some, testTokenActorProbe.ref, mockHttp)
      var firstRequest   = true
      mockHttp.responseFn = { _ =>
        Future.successful(if (firstRequest) {
          firstRequest = false
          HttpResponse(StatusCodes.Unauthorized)
        } else {
          HttpResponse(entity = HttpEntity("""{ }"""))
        })
      }

      val resultF = bigQuerySource.runWith(Sink.seq)
      testTokenActorProbe.expectMsg(TokenRequest(false))
      testTokenActorProbe.reply(TokenResponse("TOKEN"))

      testTokenActorProbe.expectMsg(TokenRequest(true))
      testTokenActorProbe.reply(TokenResponse("TOKEN"))

      Await.result(resultF, 1.second) shouldBe Seq("success")
      mockHttp.usedToken shouldBe "TOKEN"
    }

    "url encode page token" in new BigQuerySourceScope {

      val bigQuerySource = BigQueryStreamSource(HttpRequest(), _ => "success".some, testTokenActorProbe.ref, mockHttp)
      mockHttp.responseFn = { request =>
        request.uri.toString() match {
          case "/" =>
            Future.successful(
              HttpResponse(entity = HttpEntity("""{ "pageToken": "===", "jobReference": { "jobId": "job123"} }"""))
            )
          case "/job123?pageToken=%3D%3D%3D" => Future.successful(HttpResponse(entity = HttpEntity("""{ }""")))
        }
      }

      val resultF = bigQuerySource.runWith(Sink.seq)
      testTokenActorProbe.expectMsg(TokenRequest(false))
      testTokenActorProbe.reply(TokenResponse("TOKEN"))

      testTokenActorProbe.expectMsg(TokenRequest(false))
      testTokenActorProbe.reply(TokenResponse("TOKEN"))

      Await.result(resultF, 1.second) shouldBe Seq("success", "success")
      mockHttp.usedToken shouldBe "TOKEN"
    }

  }

}
