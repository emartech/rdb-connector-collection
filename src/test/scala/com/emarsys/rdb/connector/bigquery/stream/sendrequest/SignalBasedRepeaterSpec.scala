package com.emarsys.rdb.connector.bigquery.stream.sendrequest

import akka.actor.ActorSystem
import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Sink, Source}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.testkit.TestKit
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._

class SignalBasedRepeaterSpec extends TestKit(ActorSystem("SignalBasedRepeaterSpec"))
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  implicit val materializer = ActorMaterializer()
    val timeout = 300.millis

    def createTestGraph[A, B, C](source: Source[Int, A], errorSignalSource: Source[Boolean, B], sink: Sink[(Int, Boolean), C]): RunnableGraph[(A, B, C)] = {
      RunnableGraph.fromGraph(GraphDSL.create(source, errorSignalSource, sink)((_, _, _)) { implicit builder =>
        (source, errorSignalSource, sink) =>
          import GraphDSL.Implicits._

          val xfile = builder.add(new SignalBasedRepeater[Int]())

          source            ~> xfile.in0
          errorSignalSource ~> xfile.in1

          xfile.out ~> sink.in

          ClosedShape
      })
    }

  "SignalBasedRepeater" must {

    "when started, pull on both inputs" in {
      val testGraph = createTestGraph(TestSource.probe[Int], TestSource.probe[Boolean], TestSink.probe[(Int, Boolean)])
      val (dataIn, signalIn, _) = testGraph.run()

      dataIn.expectRequest() should be > 1L
      signalIn.expectRequest() should be > 1L
    }

    "when both inputs are ready and out is getting backpressued, produce an element" in {
      val testGraph = createTestGraph(TestSource.probe[Int], TestSource.probe[Boolean], TestSink.probe[(Int, Boolean)])
      val (dataIn, signalIn, sink) = testGraph.run()

      dataIn.sendNext(0)
      signalIn.sendNext(true)
      sink.requestNext() shouldBe (0, false)
    }

    "when in is ready and out is backpressued and signal is pushed, produce an element" in {
      val testGraph = createTestGraph(TestSource.probe[Int], TestSource.probe[Boolean], TestSink.probe[(Int, Boolean)])
      val (dataIn, signalIn, sink) = testGraph.run()

      dataIn.sendNext(0)
      sink.request(1)
      sink.expectNoMessage(timeout)
      signalIn.sendNext(true)
      sink.expectNext() shouldBe (0, false)
    }

    "when signal is ready and out is backpressued and in is pushed, produce an element" in {
      val testGraph = createTestGraph(TestSource.probe[Int], TestSource.probe[Boolean], TestSink.probe[(Int, Boolean)])
      val (dataIn, signalIn, sink) = testGraph.run()

      signalIn.sendNext(true)
      sink.request(1)
      sink.expectNoMessage(timeout)
      dataIn.sendNext(0)
      sink.expectNext() shouldBe (0, false)
    }

    "when signal is true, grab element from input, cache it and provide it" in {
      val testGraph = createTestGraph(TestSource.probe[Int], TestSource.probe[Boolean], TestSink.probe[(Int, Boolean)])
      val (dataIn, signalIn, sink) = testGraph.run()

      signalIn.sendNext(true)
      dataIn.sendNext(0)
      sink.requestNext() shouldBe (0, false)
    }

    "when signal is false, provide cached" in {
      val testGraph = createTestGraph(TestSource.probe[Int], TestSource.probe[Boolean], TestSink.probe[(Int, Boolean)])
      val (dataIn, signalIn, sink) = testGraph.run()

      dataIn.sendNext(0)
      signalIn.sendNext(true)
      sink.requestNext() shouldBe (0, false)

      signalIn.sendNext(false)
      sink.requestNext() shouldBe (0, true)
    }

    "when signal is false and cache is empty, throw illegal state exception" in {
      val testGraph = createTestGraph(TestSource.probe[Int], TestSource.probe[Boolean], TestSink.probe[(Int, Boolean)])
      val (dataIn, signalIn, sink) = testGraph.run()

      dataIn.sendNext(0)
      signalIn.sendNext(false)
      sink.request(1)
      sink.expectError() shouldBe a [IllegalStateException]
    }

    "when in is completed, complete the stage" in {
      val testGraph = createTestGraph(TestSource.probe[Int], TestSource.probe[Boolean], TestSink.probe[(Int, Boolean)])
      val (dataIn, signalIn, sink) = testGraph.run()

      dataIn.sendNext(0)
      signalIn.sendNext(true)
      sink.requestNext() shouldBe (0, false)

      dataIn.sendComplete()
      signalIn.sendNext(true)
      sink.request(1)

      signalIn.expectCancellation()
      sink.expectComplete()
    }

    "when fail signal is sent twice, throw exception" in {
      val testGraph = createTestGraph(TestSource.probe[Int], TestSource.probe[Boolean], TestSink.probe[(Int, Boolean)])
      val (dataIn, signalIn, sink) = testGraph.run()

      signalIn.sendNext(true)
      dataIn.sendNext(0)
      sink.requestNext()

      signalIn.sendNext(false)
      signalIn.sendNext(false)

      sink.expectError() shouldBe a [IllegalStateException]
    }

  }
}
