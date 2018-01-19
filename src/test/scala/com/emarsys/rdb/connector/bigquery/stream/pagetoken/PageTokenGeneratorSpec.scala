package com.emarsys.rdb.connector.bigquery.stream.pagetoken

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Source}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.bigquery.stream.parser.PagingInfo
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

    val emptyPagingInfo = PagingInfo(None, None)
    val pagingInfoWithPageToken = PagingInfo(Some("next page"), None)
    val pagingInfoWithJobId = PagingInfo(None, Some("job"))
    val pagingInfoWithPageTokenAndJobId = PagingInfo(Some("next page"), Some("job"))

    "always return an empty page info first" in {
      val sourceProbe = TestSource.probe[PagingInfo]
      val sinkProbe = TestSink.probe[PagingInfo]

      val probe = sourceProbe
        .via(PageTokenGenerator())
        .runWith(sinkProbe)

      probe.requestNext() should be(emptyPagingInfo)
    }

    "return a page token on second pull when there is a new page token sent" in {
      val sinkProbe = TestSink.probe[PagingInfo]

      val (_, sink) = Source.single(pagingInfoWithPageTokenAndJobId)
        .via(PageTokenGenerator())
        .toMat(sinkProbe)(Keep.both).run

      sink.requestNext() should be(emptyPagingInfo)
      sink.requestNext() should be(pagingInfoWithPageTokenAndJobId)
    }

    "return no page token and close graph when there is no page token in paging info" in {
      val sinkProbe = TestSink.probe[PagingInfo]

      val probe = Source(List[PagingInfo](pagingInfoWithPageTokenAndJobId, pagingInfoWithJobId))
        .via(PageTokenGenerator())
        .runWith(sinkProbe)

      probe.requestNext() should be(emptyPagingInfo)
      probe.requestNext() should be(pagingInfoWithPageTokenAndJobId)
      probe.expectComplete()

    }

  }

}
