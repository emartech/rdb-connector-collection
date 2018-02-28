package com.emarsys.rdb.connector.bigquery.stream.parser

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpEntity, HttpResponse}
import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Sink, Source}
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.testkit.TestKit
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import spray.json.JsObject
import cats.syntax.option._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class ParserSpec extends TestKit(ActorSystem("ParserSpec"))
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer()

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  val pageToken = "dummyToken"
  val jobId = "dummyJobId"
  val response = HttpResponse(entity = HttpEntity(s"""{"pageToken": "$pageToken", "jobReference": { "jobId": "$jobId" } }"""))
  val responseWithoutJobId = HttpResponse(entity = HttpEntity(s"""{"pageToken": "$pageToken"}"""))

  def createTestGraph[Data, S1, S2](source: Source[HttpResponse, _], dataSink: Sink[Data, S1], pageSink: Sink[PagingInfo, S2], parseFunction: JsObject => Option[Data]): RunnableGraph[(S1, S2)] = {
    RunnableGraph.fromGraph(GraphDSL.create(dataSink, pageSink)((_, _)) { implicit builder =>
      (s1, s2) =>
        import GraphDSL.Implicits._

        val parser = builder.add(Parser[Data](parseFunction))

        source ~> parser.in
        parser.out0 ~> s1
        parser.out1 ~> s2

        ClosedShape
    })
  }

  "Parser" should {

    "output the value returned by the parse function" in {
      val testGraph = createTestGraph(Source.single(response), TestSink.probe[String], TestSink.probe[PagingInfo], _ => pageToken.some)

      val (dataSink, pageSink) = testGraph.run()

      Future(pageSink.requestNext())
      val parseResultFuture = Future(dataSink.requestNext())

      val result = Await.result(parseResultFuture, 3.seconds)

      result should be(pageToken)
    }

    "output the page token and jobid parsed from the http response" in {
      val testGraph = createTestGraph(Source.single(response), TestSink.probe[JsObject], TestSink.probe[PagingInfo], x => x.some)

      val (dataSink, pageSink) = testGraph.run()

      Future(dataSink.requestNext())
      val parsePageTokenFuture = Future(pageSink.requestNext())

      val result = Await.result(parsePageTokenFuture, 3.seconds)

      result should be(PagingInfo(Some(pageToken), Some(jobId)))
    }

    "output the page token parsed from the http response" in {
      val testGraph = createTestGraph(Source.single(responseWithoutJobId), TestSink.probe[JsObject], TestSink.probe[PagingInfo], x => x.some)

      val (dataSink, pageSink) = testGraph.run()

      Future(dataSink.requestNext())
      val parsePageTokenFuture = Future(pageSink.requestNext())

      val result = Await.result(parsePageTokenFuture, 3.seconds)

      result should be(PagingInfo(Some(pageToken), None))
    }

    "output none for page token when http response does not contain page token" in {
      val testGraph = createTestGraph(Source.single(HttpResponse(entity = HttpEntity("{}"))), TestSink.probe[JsObject], TestSink.probe[PagingInfo], x => x.some)

      val (dataSink, pageSink) = testGraph.run()

      Future(dataSink.requestNext())
      val parsePageTokenFuture = Future(pageSink.requestNext())

      val result = Await.result(parsePageTokenFuture, 3.seconds)

      result should be(PagingInfo(None, None))
    }

  }
}
