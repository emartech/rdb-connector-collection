package com.emarsys.rdb.connector.mysql

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.mysql.utils.SelectDbInitHelper
import com.emarsys.rdb.connector.test.DeleteItSpec

class MySqlDeleteItSpec extends TestKit(ActorSystem("MySqlDeleteItSpec")) with DeleteItSpec with SelectDbInitHelper {
  val aTableName: String = tableName
  val bTableName: String = s"temp_$uuid"

  implicit override val materializer: Materializer = ActorMaterializer()

  override def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }

}
