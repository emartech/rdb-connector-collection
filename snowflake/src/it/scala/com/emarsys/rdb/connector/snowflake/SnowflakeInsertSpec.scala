package com.emarsys.rdb.connector.snowflake

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.snowflake.utils.SelectDbInitHelper
import com.emarsys.rdb.connector.test.InsertItSpec

import scala.concurrent.duration._

class SnowflakeInsertSpec
    extends TestKit(ActorSystem("SnowflakeInsertSpec"))
    with InsertItSpec
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
