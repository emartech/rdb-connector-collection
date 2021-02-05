package com.emarsys.rdb.connector.bigquery.stream.pagetoken

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import com.emarsys.rdb.connector.bigquery.stream.parser.PagingInfo
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

class EndOfStreamDetectorSpec
    extends TestKit(ActorSystem("PageTokenGeneratorSpec"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  override def afterAll() = {
    shutdown()
  }

  "EndOfStreamDetector" should {

    val emptyPagingInfo         = PagingInfo(None, None)
    val pagingInfoWithPageToken = PagingInfo(Some("next page"), None)

    "terminate processing if retry is false and page token is none" in {
      val sinkProbe = TestSink.probe[(Boolean, PagingInfo)]

      val probe = Source
        .single((false, emptyPagingInfo))
        .via(EndOfStreamDetector())
        .runWith(sinkProbe)

      probe.expectSubscriptionAndComplete()
    }

    "forward input if retry is true, no matter the page token" in {
      val sinkProbe = TestSink.probe[(Boolean, PagingInfo)]

      val probe = Source
        .single((true, emptyPagingInfo))
        .via(EndOfStreamDetector())
        .runWith(sinkProbe)

      probe.requestNext() should be((true, emptyPagingInfo))
    }

    "forward input if page token exists, even if retry is false" in {
      val sinkProbe = TestSink.probe[(Boolean, PagingInfo)]

      val probe = Source
        .single((false, pagingInfoWithPageToken))
        .via(EndOfStreamDetector())
        .runWith(sinkProbe)

      probe.requestNext() should be((false, pagingInfoWithPageToken))
    }

  }

}
