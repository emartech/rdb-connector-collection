package com.emarsys.rdb.connector.postgresql

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.emarsys.rdb.connector.postgresql.utils.SelectDbInitHelper
import com.emarsys.rdb.connector.test.UpdateItSpec

import scala.concurrent.duration._

class PostgreSqlUpdateItSpec
    extends TestKit(ActorSystem("PostgreSqlUpdateItSpec"))
    with UpdateItSpec
    with SelectDbInitHelper {
  val aTableName: String = tableName
  val bTableName: String = s"temp_$uuid"

  override val awaitTimeout = 15.seconds



  override def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }
}
