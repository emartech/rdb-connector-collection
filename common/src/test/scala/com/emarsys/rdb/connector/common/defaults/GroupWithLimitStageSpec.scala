package com.emarsys.rdb.connector.common.defaults

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import concurrent.duration._
import scala.concurrent.Await

class GroupWithLimitStageSpec extends WordSpecLike with Matchers with BeforeAndAfterAll{

  implicit val actorSystem = ActorSystem("GroupWithLimitStageSpec")
  implicit val materializer = ActorMaterializer()
  val timeout = 3.seconds

  override protected def afterAll(): Unit = {
    actorSystem.terminate()
    super.afterAll()
  }

  val header = Seq("id", "name", "text")
  val data1 = List(
    Seq("1", "name", "text1"),
    Seq("1", "name", "text2"),
    Seq("1", "name", "text3"),
    Seq("1", "name", "text4"),
    Seq("1", "name", "text5"),
    Seq("1", "name", "text6"),
  )
  val data2 = List(
    Seq("2", "name", "text1"),
    Seq("2", "name", "text2"),
    Seq("2", "name", "text3"),
    Seq("2", "name", "text4"),
    Seq("2", "name", "text5"),
    Seq("2", "name", "text6"),
  )

  "GroupWithLimitStage" should {

    "limit elements from the same group" in {
      val result = Await.result(Source(header :: data1).via(GroupWithLimitStage(Seq("id", "name"), 2)).runWith(Sink.seq), timeout)

      result shouldBe (header :: data1.take(2))
    }

    "limit elements from multiple groups" in {
      val result = Await.result(Source(header :: data1 ++ data2).via(GroupWithLimitStage(Seq("id", "name"), 2)).runWith(Sink.seq), timeout)

      result shouldBe (header :: data1.take(2) ++ data2.take(2))
    }

    "limit elements from multiple groups shuffled" in {
      val shuffled = data1.zip(data2).flatMap(p => List(p._1, p._2))
      val result = Await.result(Source(header :: shuffled).via(GroupWithLimitStage(Seq("id", "name"), 2)).runWith(Sink.seq), timeout)

      result shouldBe (header :: shuffled.take(4))
    }

    "emits instantly" in {
      val (sourceProbe, sinkProbe) = TestSource.probe[Seq[String]].via(GroupWithLimitStage(Seq("id", "name"), 2)).toMat(TestSink.probe[Seq[String]])(Keep.both).run()

      sinkProbe.request(2)
      sourceProbe.sendNext(header)
      sourceProbe.sendNext(data1.head)
      sinkProbe.expectNext(header)
      sinkProbe.expectNext(data1.head)
      sinkProbe.request(2)
      sourceProbe.sendNext(data1.head)
      sourceProbe.sendNext(data1.head)
      sourceProbe.sendNext(data2.head)
      sinkProbe.expectNext(data1.head)
      sinkProbe.expectNext(data2.head)
    }

    "can handle empty results" in {
      val result = Await.result(Source(List()).via(GroupWithLimitStage(Seq("id", "name"), 2)).runWith(Sink.seq), timeout)

      result shouldBe List()
    }

    "can handle oneliner results" in {
      val result = Await.result(Source(header :: Nil).via(GroupWithLimitStage(Seq("id", "name"), 2)).runWith(Sink.seq), timeout)

      result shouldBe List()
    }

    "can handle uppercase refs with lowercase headers" in {
      val result = Await.result(Source(header :: data1 ++ data2).via(GroupWithLimitStage(Seq("ID", "NAME"), 2)).runWith(Sink.seq), timeout)

      result shouldBe (header :: data1.take(2) ++ data2.take(2))
    }

    "can handle lowercase refs with uppercase headers" in {
      val upperHeader = header.map(_.toUpperCase)
      val result = Await.result(Source(upperHeader :: data1 ++ data2.take(2)).via(GroupWithLimitStage(Seq("id", "name"), 2)).runWith(Sink.seq), timeout)

      result shouldBe (upperHeader :: data1.take(2) ++ data2.take(2))
    }

  }

}
