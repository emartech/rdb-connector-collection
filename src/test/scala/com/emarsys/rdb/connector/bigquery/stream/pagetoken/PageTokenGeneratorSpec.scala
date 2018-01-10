package com.emarsys.rdb.connector.bigquery.stream.pagetoken

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.TestKit
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

class PageTokenGeneratorSpec extends TestKit(ActorSystem("PageTokenGeneratorSpec"))
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "PageTokenGenerator" should {

    implicit val materializer = ActorMaterializer()

    val pageToken = Some("next page")

    "always return an empty page token first" in {
      val sourceProbe = TestSource.probe[Option[String]]
      val sinkProbe = TestSink.probe[Option[String]]

      val probe = sourceProbe
        .via(PageTokenGenerator[String]())
        .runWith(sinkProbe)

      probe.requestNext() should be(None)
    }

    "return a page token on second pull when there is a new page token sent" in {
      val sinkProbe = TestSink.probe[Option[String]]

      val (_, sink) = Source.single(pageToken)
        .via(PageTokenGenerator[String]())
        .toMat(sinkProbe)(Keep.both).run

      sink.requestNext() should be(None)
      sink.requestNext() should be(pageToken)
    }

    "return no page token and close graph when there are no more page tokens sent" in {
      val sinkProbe = TestSink.probe[Option[String]]

      val probe = Source(List[Option[String]](pageToken, None))
        .via(PageTokenGenerator[String]())
        .runWith(sinkProbe)

      probe.requestNext() should be(None)
      probe.requestNext() should be(pageToken)
      probe.expectComplete()

    }

  }

}
