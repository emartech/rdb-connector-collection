package com.emarsys.rdb.connector.postgresql

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.emarsys.rdb.connector.postgresql.utils.SelectDbInitHelper
import com.emarsys.rdb.connector.test.DeleteItSpec

import scala.concurrent.duration._

class PostgreSqlDeleteItSpec
    extends TestKit(ActorSystem("PostgreSqlDeleteItSpec"))
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
