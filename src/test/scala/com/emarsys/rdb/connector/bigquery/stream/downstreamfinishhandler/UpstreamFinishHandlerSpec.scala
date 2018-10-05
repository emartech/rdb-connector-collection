package com.emarsys.rdb.connector.bigquery.stream.downstreamfinishhandler

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import akka.util.Timeout
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

class UpstreamFinishHandlerSpec extends TestKit(ActorSystem("UpstreamFinishHandlerSpec"))
  with WordSpecLike
  with Matchers
  with MockitoSugar {

  implicit val materializer = ActorMaterializer()
  implicit val timeout = Timeout(1.second)

  trait TestScope {

    var calledParams: Option[Int] = None

    val dummyHandlerCallback = (x: Int) => {
      calledParams = Some(x)
    }

    val timeOutHandler = UpstreamFinishHandler[Int](dummyHandlerCallback)
  }

  "UpstreamFinishHandlerSpec" must {

    "Do not call handler if there was no timeout" in new TestScope {
      val result =
        Source.repeat(1)
          .via(timeOutHandler)
          .take(10)
          .runWith(Sink.ignore)

      Try(Await.result(result, 3.seconds))

      calledParams shouldBe None

    }

    "Do not call handler if there was no data and cancel was pushed" in new TestScope {
      val result =
        Source.failed(new Exception)
          .via(timeOutHandler)
          .take(5)
          .runWith(Sink.ignore)

      Try(Await.result(result, 3.seconds))

      calledParams shouldBe None
    }

    "Call handler if there was data and cancel was pushed" in new TestScope {
      val result =
        Source(1 to 3)
          .via(timeOutHandler)
          .runWith(Sink.ignore)

      Try(Await.result(result, 3.seconds))


      calledParams shouldBe Some(3)
    }
  }

}
