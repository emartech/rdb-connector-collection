package com.emarsys.rdb.connector.bigquery.stream

import akka.actor.{ActorSystem, ExtendedActorSystem}
import akka.event.LoggingAdapter
import akka.http.scaladsl.model._
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.http.scaladsl.{HttpExt, HttpsConnectionContext}
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import cats.syntax.option._
import com.emarsys.rdb.connector.bigquery.GoogleSession
import com.emarsys.rdb.connector.bigquery.stream.parser.PagingInfo
import org.mockito.captor.ArgCaptor
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import spray.json.JsObject

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

class BigQueryStreamSourceSpec
    extends TestKit(ActorSystem("BigQueryStreamSourceSpec"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ArgumentMatchersSugar
    with IdiomaticMockito {

  override def afterAll(): Unit = {
    shutdown()
  }

  val timeout                   = 3.seconds
  implicit val executionContext = system.dispatcher

  trait Scope {
    val session = mock[GoogleSession]
    session.getToken() returns Future.successful("TOKEN")

    val mockHttp = mock[HttpExt]
    mockHttp.system returns mock[ExtendedActorSystem]

    def mockSingleRequest =
      mockHttp.singleRequest(
        any[HttpRequest],
        any[HttpsConnectionContext],
        any[ConnectionPoolSettings],
        any[LoggingAdapter]
      )
  }

  def getToken(request: HttpRequest): String = {
    request.getHeader("Authorization").get().value().stripPrefix("Bearer ")
  }

  "BigQueryStreamSource" should {

    "return single page request" in new Scope {
      mockSingleRequest returns Future.successful(
        HttpResponse(entity = HttpEntity("{}"))
      )

      val callback = spyLambda((x: (Boolean, PagingInfo)) => ())

      val bigQuerySource =
        BigQueryStreamSource(HttpRequest(), _ => "success".some, session, mockHttp, callback)

      val resultF = Source.fromGraph(bigQuerySource).runWith(Sink.head)

      Await.result(resultF, timeout) shouldBe "success"
      session.getToken() was called
      val captor = ArgCaptor[HttpRequest]
      mockHttp.singleRequest(captor, any[HttpsConnectionContext], any[ConnectionPoolSettings], any[LoggingAdapter]) was called
      getToken(captor.value) shouldBe "TOKEN"
      callback.apply(any[(Boolean, PagingInfo)]) wasNever called
    }

    "return two page request" in new Scope {
      mockSingleRequest answers (
          (request: HttpRequest) =>
            request.uri.toString() match {
              case "/" =>
                Future.successful(
                  HttpResponse(
                    entity = HttpEntity("""{ "pageToken": "nextPage", "jobReference": { "jobId": "job123"} }""")
                  )
                )
              case "/job123?pageToken=nextPage" =>
                Future.successful(HttpResponse(entity = HttpEntity("""{ }""")))
            }
        )

      val bigQuerySource = BigQueryStreamSource(HttpRequest(), _ => "success".some, session, mockHttp)

      val resultF = bigQuerySource.runWith(Sink.seq)

      Await.result(resultF, timeout) shouldBe Seq("success", "success")
      session.getToken() wasCalled 2.times
      val captor = ArgCaptor[HttpRequest]
      mockHttp.singleRequest(captor, any[HttpsConnectionContext], any[ConnectionPoolSettings], any[LoggingAdapter]) wasCalled 2.times
      captor.values.map(getToken) shouldBe List("TOKEN", "TOKEN")
    }

    "url encode page token" in new Scope {
      val bigQuerySource = BigQueryStreamSource(HttpRequest(), _ => "success".some, session, mockHttp, x => ())
      mockSingleRequest answers (
          (request: HttpRequest) =>
            request.uri.toString() match {
              case "/" =>
                Future.successful(
                  HttpResponse(entity = HttpEntity("""{ "pageToken": "===", "jobReference": { "jobId": "job123"} }"""))
                )
              case "/job123?pageToken=%3D%3D%3D" => Future.successful(HttpResponse(entity = HttpEntity("""{ }""")))
            }
        )

      val resultF = bigQuerySource.runWith(Sink.seq)

      Await.result(resultF, timeout) shouldBe Seq("success", "success")
      session.getToken() wasCalled 2.times
      val captor = ArgCaptor[HttpRequest]
      mockHttp.singleRequest(captor, any[HttpsConnectionContext], any[ConnectionPoolSettings], any[LoggingAdapter]) wasCalled 2.times
      captor.values.map(getToken) shouldBe List("TOKEN", "TOKEN")
    }

    "get first page again when parsing returns None" in new Scope {
      mockSingleRequest answers (
          (request: HttpRequest) =>
            request.uri.toString() match {
              case "/" =>
                Future.successful(
                  HttpResponse(
                    entity = HttpEntity("""{ "pageToken": "nextPage", "jobReference": { "jobId": "job123"} }""")
                  )
                )
              case "/job123" =>
                Future.successful(HttpResponse(entity = HttpEntity("""{ }""")))
            }
        )
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
      session.getToken() wasCalled 2.times
      val captor = ArgCaptor[HttpRequest]
      mockHttp.singleRequest(captor, any[HttpsConnectionContext], any[ConnectionPoolSettings], any[LoggingAdapter]) wasCalled 2.times
      captor.values.map(getToken) shouldBe List("TOKEN", "TOKEN")
    }

    "call upstreamFinishHandler function when stream is cancelled" in new Scope {
      mockSingleRequest answers Future({
        Thread.sleep(700)
        HttpResponse(entity = HttpEntity("{}"))
      })

      @volatile
      var isDummyHandlerCallbackCalled = false

      val dummyHandlerCallback = (x: (Boolean, PagingInfo)) => isDummyHandlerCallbackCalled = true

      val bigQuerySource =
        BigQueryStreamSource(HttpRequest(), _ => Option.empty[String], session, mockHttp, dummyHandlerCallback)

      val resultF = bigQuerySource.completionTimeout(1.second).runWith(Sink.ignore)

      Try(Await.result(resultF, timeout))
      Thread.sleep(500)

      isDummyHandlerCallbackCalled shouldBe true
    }

  }

}
