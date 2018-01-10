package com.emarsys.rdb.connector.bigquery.stream.sendrequest

import akka.actor.ActorSystem
import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Sink, Source}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.testkit.TestKit
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._

class SignalBasedRepeaterSpec extends TestKit(ActorSystem("SignalBasedRepeaterSpec"))
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  implicit val materializer = ActorMaterializer()

    def createTestGraph[A, B, C](source: Source[Int, A], errorSignalSource: Source[Unit, B], sink: Sink[(Int, Boolean), C]): RunnableGraph[(A, B, C)] = {
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

    "without signal" in {
      val testGraph = createTestGraph(Source(0 until 4), TestSource.probe[Unit], Sink.seq[(Int, Boolean)])
      val (_, _, sink) = testGraph.run()

      Await.result(sink, 1.second) shouldEqual (0 until 4).map((_, false))
    }

    "replay source when got signal" in {
      val testGraph = createTestGraph(Source(1 until 4), TestSource.probe[Unit], TestSink.probe[(Int, Boolean)])
      val (_, errorSignal, sink) = testGraph.run()

      sink.requestNext((1, false))
      sink.requestNext((2, false))

      errorSignal.sendNext({})
      sink.requestNext((2, true))

      errorSignal.sendNext({})
      sink.requestNext((2, true))

      sink.requestNext((3, false))
    }

    "illegal state exception if first was signal" in {
      val testGraph = createTestGraph(TestSource.probe[Int], TestSource.probe[Unit], Sink.last[(Int, Boolean)])

      val (_, errorSignal, sink) = testGraph.run()
      errorSignal.sendNext({})

      assertThrows[IllegalStateException] {
        Await.result(sink, 1.second)
      }
    }

    "illegal state exception if multiple signal" in {
      val testGraph = createTestGraph(TestSource.probe[Int], TestSource.probe[Unit], Sink.last[(Int, Boolean)])

      val (source, errorSignal, sink) = testGraph.run()

      source.sendNext(1)
      errorSignal.sendNext({})
      errorSignal.sendNext({})

      assertThrows[IllegalStateException] {
        Await.result(sink, 1.second)
      }
    }

  }
}
