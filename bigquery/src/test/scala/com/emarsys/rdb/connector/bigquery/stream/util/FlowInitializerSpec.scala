package com.emarsys.rdb.connector.bigquery.stream.util

import akka.actor.ActorSystem
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.TestKit
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

class FlowInitializerSpec
    extends TestKit(ActorSystem("FlowInitializerSpec"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  override def afterAll() = {
    shutdown()
  }

  "FlowInitializer" should {

    "Put initial value in front of the stream" in {
      val sourceProbe = TestSource.probe[String]
      val sinkProbe = TestSink.probe[String]

      val probe = sourceProbe
        .via(FlowInitializer("a"))
        .runWith(sinkProbe)

      probe.requestNext() should be("a")
    }
  }

}
