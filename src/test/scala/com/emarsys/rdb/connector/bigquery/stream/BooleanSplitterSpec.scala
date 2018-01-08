package com.emarsys.rdb.connector.bigquery.stream

import akka.actor.ActorSystem
import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Sink, Source}
import akka.stream.testkit.{TestPublisher, TestSubscriber}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.testkit.TestKit
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.collection.immutable
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.Try

class BooleanSplitterSpec extends TestKit(ActorSystem("BooleanSplitterSpec"))
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  implicit val materializer = ActorMaterializer()


  def createTestGraph[T](source: Source[Int, _], sink1: Sink[Int, T], sink2: Sink[Int, T]): RunnableGraph[(T, T)] = {
    RunnableGraph.fromGraph(GraphDSL.create(sink1, sink2)((_, _)) { implicit builder =>
      (s1, s2) =>
        import GraphDSL.Implicits._

        val splitter = builder.add(BooleanSplitter[Int](_ < 3))

        source ~> splitter.in
        splitter.out(0) ~> s1
        splitter.out(1) ~> s2

        ClosedShape
    })
  }

  "BooleanSplitter" must {

    "input splitting" in {
      val testGraph = createTestGraph(Source(0 until 10), Sink.seq, Sink.seq)

      val (trueResult, falseResult) = testGraph.run()

      Await.result(trueResult, 1.second) shouldEqual (0 until 3)
      Await.result(falseResult, 1.second) shouldEqual (3 until 10)
    }


    "backpressure only if all outputs are pulled - if only requested from true" in {
      val testGraph = createTestGraph(Source.single(2), TestSink.probe, TestSink.probe)

      val (trueSink, falseSink) = testGraph.run()

      trueSink.request(1)

      Try(trueSink.expectNext(20.milliseconds)).toOption shouldEqual None

      falseSink.request(1)

      trueSink.expectNext(2)

    }

    "backpressure only if all outputs are pulled - if only requested from false" in {
      val testGraph = createTestGraph(Source.single(4), TestSink.probe, TestSink.probe)

      val (trueSink, falseSink) = testGraph.run()

      falseSink.request(1)

      Try(falseSink.expectNext(20.milliseconds)).toOption shouldEqual None

      trueSink.request(1)

      falseSink.expectNext(4)

    }

  }
}
