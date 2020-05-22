package com.emarsys.rdb.connector.mssql

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.mssql.utils.SelectDbInitHelper
import com.emarsys.rdb.connector.test.DeleteItSpec

class MsSqlDeleteItSpec extends TestKit(ActorSystem("MsSqlDeleteItSpec")) with DeleteItSpec with SelectDbInitHelper {
  val aTableName: String = tableName
  val bTableName: String = s"temp_$uuid"

  implicit override val materializer: Materializer = ActorMaterializer()

  override def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }

}
