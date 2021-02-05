package com.emarsys.rdb.connector.mysql

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.emarsys.rdb.connector.mysql.utils.SelectDbInitHelper
import com.emarsys.rdb.connector.test.ReplaceItSpec

class MySqlReplaceItSpec extends TestKit(ActorSystem("MySqlReplaceItSpec")) with ReplaceItSpec with SelectDbInitHelper {
  val aTableName: String = tableName
  val bTableName: String = s"temp_$uuid"



  override def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }

}
