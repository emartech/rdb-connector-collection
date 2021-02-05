package com.emarsys.rdb.connector.snowflake

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.emarsys.rdb.connector.snowflake.utils.SelectDbInitHelper
import com.emarsys.rdb.connector.test.ReplaceItSpec

import scala.concurrent.duration._

class SnowflakeReplaceItSpec
    extends TestKit(ActorSystem("SnowflakeReplaceItSpec"))
    with ReplaceItSpec
    with SelectDbInitHelper {
  val aTableName: String = tableName
  val bTableName: String = s"temp_$uuid"

  override val awaitTimeout = 15.seconds



  override def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }

}
