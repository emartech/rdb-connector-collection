package com.emarsys.rdb.connector.bigquery.stream

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
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

  implicit val materializer = ActorMaterializer()


  "ErrorSignalProcessor" must {

    "Send signal if auth error" in {
      val resultF = Source.single(HttpResponse(status = StatusCodes.Unauthorized))
        .via(ErrorSignalProcessor())
        .runWith(Sink.last)

      Await.result(resultF, 1.second) shouldEqual ()
    }

    "Not send signal if auth error" in {
      val resultF = Source.single(HttpResponse(status = StatusCodes.Forbidden))
        .via(ErrorSignalProcessor())
        .runWith(Sink.seq)

      Await.result(resultF, 1.second) shouldEqual Seq.empty
    }

    "Not send signal if success" in {
      val resultF = Source.single(HttpResponse())
        .via(ErrorSignalProcessor())
        .runWith(Sink.seq)

      Await.result(resultF, 1.second) shouldEqual Seq.empty
    }

  }
}
