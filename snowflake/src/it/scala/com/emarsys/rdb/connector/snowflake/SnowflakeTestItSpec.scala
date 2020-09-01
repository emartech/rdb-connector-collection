package com.emarsys.rdb.connector.snowflake

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.emarsys.rdb.connector.snowflake.utils.BaseDbSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.Await
import scala.concurrent.duration._

class SnowflakeTestItSpec
    extends TestKit(ActorSystem("SnowflakeTestItSpec"))
    with AnyWordSpecLike
    with Matchers
    with BaseDbSpec {

  "#test" should {
    "test the connection" in {
      val result  = Await.result(connector.testConnection(), 5.seconds)
      result shouldBe Right(())
    }
  }
}
