package com.emarsys.rdb.connector.bigquery.stream.sendrequest

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.TestKit
import akka.util.Timeout
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._

class ErrorSignalProcessorSpec extends TestKit(ActorSystem("ErrorSignalProcessorSpec"))
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val timeout: Timeout = Timeout(1.second)

  "ErrorSignalProcessor" must {

    "Send true at beginning" in {
      val resultF = Source.single(HttpResponse(status = StatusCodes.Unauthorized))
        .via(ErrorSignalProcessor())
        .runWith(Sink.seq)

      Await.result(resultF, 1.second) shouldEqual List(true, false)
    }

    "Send true if OK" in {
      val resultF = Source.single(HttpResponse(status = StatusCodes.OK))
        .via(ErrorSignalProcessor())
        .runWith(Sink.seq)

      Await.result(resultF, 1.second) shouldEqual List(true, true)
    }

    "Send false if auth error" in {
      val resultF = Source.single(HttpResponse(status = StatusCodes.Unauthorized))
        .via(ErrorSignalProcessor())
        .runWith(Sink.seq)

      Await.result(resultF, 1.second) shouldEqual List(true, false)
    }

    "Throw exepction if other error" in {
      val ts = TestSource.probe[HttpResponse]
      val (source, sink) = ts
        .via(ErrorSignalProcessor())
        .toMat(TestSink.probe[Boolean])(Keep.both)
        .run


      sink.requestNext() shouldBe true
      source.sendNext(HttpResponse(status = StatusCodes.Forbidden))
      sink.request(0)
      sink.expectError() shouldBe a[IllegalStateException]
    }
  }
}
