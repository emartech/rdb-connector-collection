package com.emarsys.rdb.connector.bigquery.stream.util

import akka.actor.ActorSystem
import akka.stream.ClosedShape
import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Sink, Source}
import akka.testkit.TestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.Await
import scala.concurrent.duration._

class SplitterSpec extends TestKit(ActorSystem("SplitterSpec")) with AnyWordSpecLike with Matchers with BeforeAndAfterAll {

  override def afterAll() = {
    shutdown()
  }

  def createTestGraph[T](source: Source[Int, _], sink1: Sink[Int, T], sink2: Sink[Int, T]): RunnableGraph[(T, T)] = {
    RunnableGraph.fromGraph(GraphDSL.create(sink1, sink2)((_, _)) { implicit builder => (s1, s2) =>
      import GraphDSL.Implicits._

      val splitter = builder.add(Splitter[Int](_ < 3)())

      source ~> splitter.in
      splitter.out(0) ~> s1
      splitter.out(1) ~> s2

      ClosedShape
    })
  }

  "Splitter" must {

    "input splitting" in {
      val testGraph = createTestGraph(Source(0 until 10), Sink.seq, Sink.seq)

      val (trueResult, allResult) = testGraph.run()

      Await.result(trueResult, 1.second) shouldEqual (0 until 3)
      Await.result(allResult, 1.second) shouldEqual (3 until 10)
    }

  }
}
