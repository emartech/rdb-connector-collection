package com.emarsys.rdb.connector.mysql

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.emarsys.rdb.connector.mysql.utils.SelectDbInitHelper
import com.emarsys.rdb.connector.test.DeleteItSpec

class MySqlDeleteItSpec extends TestKit(ActorSystem("MySqlDeleteItSpec")) with DeleteItSpec with SelectDbInitHelper {
  val aTableName: String = tableName
  val bTableName: String = s"temp_$uuid"



  override def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }

}
