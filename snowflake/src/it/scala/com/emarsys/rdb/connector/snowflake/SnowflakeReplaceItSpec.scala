package com.emarsys.rdb.connector.snowflake

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.snowflake.utils.SelectDbInitHelper
import com.emarsys.rdb.connector.test.ReplaceItSpec

import scala.concurrent.duration._

// TODO: CDP-1102 fix this test. Fixing the type of A3 in SelectDbInitHelper to 'boolean' would fix this, but would also break other tests
@org.scalatest.Ignore
class SnowflakeReplaceItSpec
    extends TestKit(ActorSystem("SnowflakeReplaceItSpec"))
    with ReplaceItSpec
    with SelectDbInitHelper {
  val aTableName: String = tableName
  val bTableName: String = s"temp_$uuid"

  override val awaitTimeout = 15.seconds

  implicit override val materializer: Materializer = ActorMaterializer()

  override def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }

}
