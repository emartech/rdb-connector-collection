package com.emarsys.rdb.connector.bigquery.stream.downstreamfinishhandler

import java.util.concurrent.TimeoutException

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import akka.util.Timeout
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

class UpstreamFailureHandlerSpec
    extends TestKit(ActorSystem("UpstreamFinishHandlerSpec"))
    with WordSpecLike
    with Matchers
    with MockitoSugar
    with BeforeAndAfterAll {

  implicit val timeout = Timeout(1.second)

  override protected def afterAll(): Unit = {
    shutdown()
  }

  trait TestScope {

    var calledParams: Option[Int] = None

    val dummyHandlerCallback = (x: Int) => {
      calledParams = Some(x)
    }

    val failureHandler = UpstreamFailureHandler[Int](dummyHandlerCallback)
  }

  "UpstreamFailureHandlerSpec" must {

    "Do not call handler if there was no timeout and downstream stops" in new TestScope {
      val result =
        Source
          .repeat(1)
          .via(failureHandler)
          .take(10)
          .runWith(Sink.ignore)

      Await.result(result, 3.seconds)

      calledParams shouldBe None
    }

    "Do not call handler if there was no timeout and upstream stops" in new TestScope {
      val result =
        Source(List(1, 2, 3))
          .via(failureHandler)
          .runWith(Sink.ignore)

      Try(Await.result(result, 3.seconds))

      calledParams shouldBe None
    }

    "Do not call handler if there was no data and cancel was pushed" in new TestScope {
      val result =
        Source(1 to 3)
          .delay(100.millis)
          .via(failureHandler)
          .completionTimeout(10.millis)
          .runWith(Sink.ignore)

      Try(Await.result(result, 3.seconds))

      calledParams shouldBe None
    }

    "Call handler if there was data and upstream fails with timeout" in new TestScope {
      val result =
        Source
          .fromIterator(() =>
            Iterator.iterate(0) { e =>
              if (e < 1) e + 1 else throw new TimeoutException("Request timeout")
            }
          )
          .via(failureHandler)
          .runWith(Sink.ignore)

      Try(Await.result(result, 3.seconds))

      calledParams shouldBe Some(1)
    }

    "Call handler if there was data and timeout happens" in new TestScope {
      val result =
        Source(1 to 3)
          .throttle(1, 100.millis)
          .via(failureHandler)
          .completionTimeout(150.millis)
          .runWith(Sink.ignore)

      Try(Await.result(result, 3.seconds))

      calledParams shouldBe Some(2)
    }
  }

}
