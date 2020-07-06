package com.emarsys.rdb.connector.snowflake

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.emarsys.rdb.connector.snowflake.utils.BaseDbSpec
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._

class SnowflakeTestItSpec
    extends TestKit(ActorSystem("SnowflakeTestItSpec"))
    with WordSpecLike
    with Matchers
    with BaseDbSpec {

  "#test" should {
    "test the connection" in {
      val result  = Await.result(connector.testConnection(), 5.seconds)
      result shouldBe Right(())
    }
  }
}
