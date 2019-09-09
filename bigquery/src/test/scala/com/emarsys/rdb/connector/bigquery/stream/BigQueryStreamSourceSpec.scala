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
import com.emarsys.rdb.connector.bigquery.stream.parser.PagingInfo
import org.mockito.Mockito.{when, _}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import spray.json.JsObject

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.reflectiveCalls
import scala.util.Try

class BigQueryStreamSourceSpec
    extends TestKit(ActorSystem("BigQueryStreamSourceSpec"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with MockitoSugar {

  override def afterAll(): Unit = {
    shutdown()
  }

  val timeout                   = 3.seconds
  implicit val materializer     = ActorMaterializer()
  implicit val executionContext = system.dispatcher

  trait Scope {
    val session = mock[GoogleSession]
    when(session.getToken()) thenReturn Future.successful("TOKEN")
  }

  var responseFnSlow: HttpRequest => Future[HttpResponse] = { _ =>
    Future({
      Thread.sleep(700)
      HttpResponse(entity = HttpEntity("{}"))
    })
  }

  var responseFnQuick: HttpRequest => Future[HttpResponse] = { _ =>
    Future.successful(HttpResponse(entity = HttpEntity("{}")))
  }

  val mockHttp =
    new HttpExt(null) {
      var usedToken                                       = ""
      var responseFn: HttpRequest => Future[HttpResponse] = responseFnQuick

      override def singleRequest(
          request: HttpRequest,
          connectionContext: HttpsConnectionContext,
          settings: ConnectionPoolSettings,
          log: LoggingAdapter
      )(implicit fm: Materializer): Future[HttpResponse] = {
        usedToken = request.headers.head.value().stripPrefix("Bearer ")
        responseFn(request)
      }
    }

  "BigQueryStreamSource" should {

    "return single page request" in new Scope {

      var isDummyHandlerCallbackCalled = false

      val dummyHandlerCallback = (x: (Boolean, PagingInfo)) => isDummyHandlerCallbackCalled = true

      val bigQuerySource =
        BigQueryStreamSource(HttpRequest(), _ => "success".some, session, mockHttp, dummyHandlerCallback)

      val resultF = Source.fromGraph(bigQuerySource).runWith(Sink.head)

      Await.result(resultF, timeout) shouldBe "success"
      verify(session).getToken()
      mockHttp.usedToken shouldBe "TOKEN"

      Await.result(Future(Thread.sleep(500)), timeout)

      isDummyHandlerCallbackCalled shouldBe false
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

    "url encode page token" in new Scope {

      val bigQuerySource = BigQueryStreamSource(HttpRequest(), _ => "success".some, session, mockHttp, x => ())
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

      val bigQuerySource = BigQueryStreamSource(HttpRequest(), parseFn, session, mockHttp, x => ())

      val resultF = bigQuerySource.runWith(Sink.seq)

      Await.result(resultF, timeout) shouldBe Seq("success")
      verify(session, times(2)).getToken()
      mockHttp.usedToken shouldBe "TOKEN"
    }

    "call upstreamFinishHandler function when stream is cancelled" in new Scope {

      mockHttp.responseFn = responseFnSlow

      var isDummyHandlerCallbackCalled = false

      val dummyHandlerCallback = (x: (Boolean, PagingInfo)) => isDummyHandlerCallbackCalled = true

      val bigQuerySource =
        BigQueryStreamSource(HttpRequest(), _ => Option.empty[String], session, mockHttp, dummyHandlerCallback)

      val resultF = bigQuerySource.completionTimeout(1.second).runWith(Sink.ignore)

      Try(Await.result(resultF, timeout))
      Await.result(Future(Thread.sleep(500)), timeout)

      isDummyHandlerCallbackCalled shouldBe true
    }

  }

}
