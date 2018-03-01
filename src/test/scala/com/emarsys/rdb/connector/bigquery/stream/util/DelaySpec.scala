package com.emarsys.rdb.connector.bigquery.stream.util

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit._
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._

class DelaySpec
    extends TestKit(ActorSystem("PageTokenGeneratorSpec"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  implicit val materializer: ActorMaterializer = ActorMaterializer()

  "Delay" should {
    "work as passthrough if should delay returns false" in {

      val elems = 0 to 12
      val probe = Source(elems)
        .map(_ => System.nanoTime())
        .via(Delay(_ => false, TimeUnit.MILLISECONDS))
        .map(start => System.nanoTime() - start)
        .runWith(TestSink.probe)

      val expectedDelay = 100.milli.dilated

      elems.foreach(_ => {
        val next = probe
          .request(1)
          .expectNext(expectedDelay)

        next should be <= expectedDelay.toNanos
      })

    }

    "delay elements using fibonacci series" in {

      val elems = 0 to 12
      val delays = List(1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144).map(_.millis)
      val probe = Source(elems)
        .map(_ => System.nanoTime())
        .via(Delay(_ => true, TimeUnit.MILLISECONDS))
        .map(start => System.nanoTime() - start)
        .runWith(TestSink.probe)

      (elems zip delays).foreach{case (_, delay) =>
        val next = probe
          .request(1)
          .expectNext()

        next should be >= delay.toNanos
      }

    }
  }

}
