package com.emarsys.rdb.connector.mssql

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.emarsys.rdb.connector.mssql.utils.SelectDbInitHelper
import com.emarsys.rdb.connector.test.UpdateItSpec

class MsSqlUpdateItSpec extends TestKit(ActorSystem("MsSqlUpdateItSpec")) with UpdateItSpec with SelectDbInitHelper {
  val aTableName: String = tableName
  val bTableName: String = s"temp_$uuid"

  override def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }

}
