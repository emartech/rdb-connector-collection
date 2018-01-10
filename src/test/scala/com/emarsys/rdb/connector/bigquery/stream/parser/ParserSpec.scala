package com.emarsys.rdb.connector.bigquery.stream.parser

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpEntity, HttpResponse}
import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Sink, Source}
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.testkit.TestKit
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import spray.json.JsObject

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

  val data = "dummy data"
  val response = HttpResponse(entity = HttpEntity(s"""{"nextPageToken": "$data" }"""))

  def createTestGraph[Data, S1, S2](source: Source[HttpResponse, _], dataSink: Sink[Data, S1], pageSink: Sink[Option[String], S2], parseFunction: JsObject => Data): RunnableGraph[(S1, S2)] = {
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
      val testGraph = createTestGraph(Source.single(response), TestSink.probe[String], TestSink.probe[Option[String]], _ => data)

      val (dataSink, pageSink) = testGraph.run()

      Future(pageSink.requestNext())
      val parseResultFuture = Future(dataSink.requestNext())

      val result = Await.result(parseResultFuture, 3.seconds)

      result should be(data)
    }

    "output the page token parsed from the http response" in {
      val testGraph = createTestGraph(Source.single(response), TestSink.probe[JsObject], TestSink.probe[Option[String]], identity)

      val (dataSink, pageSink) = testGraph.run()

      Future(dataSink.requestNext())
      val parsePageTokenFuture = Future(pageSink.requestNext())

      val result = Await.result(parsePageTokenFuture, 3.seconds)

      result should be(Some(data))
    }

    "output none for page token when http response does not contain page token" in {
      val testGraph = createTestGraph(Source.single(HttpResponse(entity = HttpEntity("{}"))), TestSink.probe[JsObject], TestSink.probe[Option[String]], identity)

      val (dataSink, pageSink) = testGraph.run()

      Future(dataSink.requestNext())
      val parsePageTokenFuture = Future(pageSink.requestNext())

      val result = Await.result(parsePageTokenFuture, 3.seconds)

      result should be(None)
    }

  }
}
