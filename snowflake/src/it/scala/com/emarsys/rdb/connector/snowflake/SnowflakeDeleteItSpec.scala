package com.emarsys.rdb.connector.snowflake

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.emarsys.rdb.connector.snowflake.utils.SelectDbInitHelper
import com.emarsys.rdb.connector.test.DeleteItSpec

import scala.concurrent.duration._

class SnowflakeDeleteItSpec
    extends TestKit(ActorSystem("SnowflakeDeleteItSpec"))
    with DeleteItSpec
    with SelectDbInitHelper {
  val aTableName: String = tableName
  val bTableName: String = s"temp_$uuid"

  override val awaitTimeout = 15.seconds



  override def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }

}
