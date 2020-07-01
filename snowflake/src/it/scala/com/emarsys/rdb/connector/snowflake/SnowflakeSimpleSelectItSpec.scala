package com.emarsys.rdb.connector.snowflake

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.snowflake.utils.{SelectDbInitHelper, TestHelper}
import com.emarsys.rdb.connector.test.SimpleSelectItSpec

import scala.concurrent.Await
import scala.concurrent.duration._


class SnowflakeSimpleSelectItSpec
  extends TestKit(ActorSystem("MySqlSimpleSelectItSpec"))
  with SimpleSelectItSpec
  with SelectDbInitHelper {

  implicit override val materializer: Materializer = ActorMaterializer()

  override def initDb(): Unit = {
    super.initDb()

    val createCTableSql =
      s"""CREATE TABLE `$cTableName` (
         |    C varchar(255) NOT NULL
         |);""".stripMargin

    val insertCDataSql =
      s"""INSERT INTO `$cTableName` (C) VALUES
         |('c12'),
         |('c12'),
         |('c3')
         |;""".stripMargin

    Await.result(for {
      _ <- TestHelper.executeQuery(createCTableSql)
      _ <- TestHelper.executeQuery(insertCDataSql)
    } yield (), 5.seconds)
  }

  override def cleanUpDb(): Unit = {
    val dropCTableSql = s"""DROP TABLE `$cTableName`;"""
    Await.result(TestHelper.executeQuery(dropCTableSql), 5.seconds)
    super.cleanUpDb()
  }
}
