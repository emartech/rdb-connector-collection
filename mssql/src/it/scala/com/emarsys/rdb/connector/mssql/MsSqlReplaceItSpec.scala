package com.emarsys.rdb.connector.mssql

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.emarsys.rdb.connector.mssql.utils.SelectDbInitHelper
import com.emarsys.rdb.connector.test.ReplaceItSpec

class MsSqlReplaceItSpec extends TestKit(ActorSystem("MsSqlReplaceItSpec")) with ReplaceItSpec with SelectDbInitHelper {
  val aTableName: String = tableName
  val bTableName: String = s"temp_$uuid"



  override def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }

}
