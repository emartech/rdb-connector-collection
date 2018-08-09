package com.emarsys.rdb.connector.bigquery.stream

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.http.scaladsl.model._
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.http.scaladsl.{HttpExt, HttpsConnectionContext}
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import cats.syntax.option._
import com.emarsys.rdb.connector.bigquery.GoogleSession
import org.mockito.Mockito.{when, _}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import spray.json.JsObject

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.reflectiveCalls

class BigQueryStreamSourceSpec
  extends TestKit(ActorSystem("BigQueryStreamSourceSpec"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with MockitoSugar {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  val timeout = 3.seconds
  implicit val materializer = ActorMaterializer()

  trait Scope {
    val session = mock[GoogleSession]
    when(session.getToken()) thenReturn Future.successful("TOKEN")
  }

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

    "return single page request" in new Scope {

      val bigQuerySource = BigQueryStreamSource(HttpRequest(), _ => "success".some, session, mockHttp)

      val resultF = Source.fromGraph(bigQuerySource).runWith(Sink.head)

      Await.result(resultF, timeout) shouldBe "success"
      verify(session).getToken()
      mockHttp.usedToken shouldBe "TOKEN"
    }

    "return two page request" in new Scope {

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
      val bigQuerySource = BigQueryStreamSource(HttpRequest(), _ => "success".some, session, mockHttp)

      val resultF = bigQuerySource.runWith(Sink.seq)

      Await.result(resultF, timeout) shouldBe Seq("success", "success")
      verify(session, times(2)).getToken()
      mockHttp.usedToken shouldBe "TOKEN"
    }

    "retry on failed auth" in new Scope {

      val bigQuerySource = BigQueryStreamSource(HttpRequest(), _ => "success".some, session, mockHttp)
      var firstRequest = true
      mockHttp.responseFn = { _ =>
        Future.successful(if (firstRequest) {
          firstRequest = false
          HttpResponse(StatusCodes.Unauthorized)
        } else {
          HttpResponse(entity = HttpEntity("""{ }"""))
        })
      }

      val resultF = bigQuerySource.runWith(Sink.seq)

      Await.result(resultF, timeout) shouldBe Seq("success")
      verify(session, times(2)).getToken()
      mockHttp.usedToken shouldBe "TOKEN"
    }

    "url encode page token" in new Scope {

      val bigQuerySource = BigQueryStreamSource(HttpRequest(), _ => "success".some, session, mockHttp)
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

      Await.result(resultF, timeout) shouldBe Seq("success", "success")
      verify(session, times(2)).getToken()
      mockHttp.usedToken shouldBe "TOKEN"
    }

    "get first page again when parsing returns None" in new Scope {
      mockHttp.responseFn = { request =>
        request.uri.toString() match {
          case "/" =>
            Future.successful(
              HttpResponse(entity = HttpEntity("""{ "pageToken": "nextPage", "jobReference": { "jobId": "job123"} }"""))
            )
          case "/job123" =>
            Future.successful(HttpResponse(entity = HttpEntity("""{ }""")))
        }
      }
      val parseFn: JsObject => Option[String] = {
        var firstCall = true
        _ => {
          if (firstCall) {
            firstCall = false
            None
          } else {
            "success".some
          }
        }
      }

      val bigQuerySource = BigQueryStreamSource(HttpRequest(), parseFn, session, mockHttp)

      val resultF = bigQuerySource.runWith(Sink.seq)

      Await.result(resultF, timeout) shouldBe Seq("success")
      verify(session, times(2)).getToken()
      mockHttp.usedToken shouldBe "TOKEN"
    }

  }

}
