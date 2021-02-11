package com.emarsys.rdb.connector.hana

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.emarsys.rdb.connector.hana.utils.{SelectDbInitHelper, TestHelper}
import com.emarsys.rdb.connector.test.SimpleSelectItSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Await
import scala.concurrent.duration._

class HanaSimpleSelectItSpec
    extends TestKit(ActorSystem("HanaSimpleSelectItSpec"))
    with SimpleSelectItSpec
    with SelectDbInitHelper
    with Matchers {

  override val booleanValue0 = "false"
  override val booleanValue1 = "true"

  override def initDb(): Unit = {
    super.initDb()

    val createCTableSql =
      s"""CREATE TABLE "$cTableName" (
         |    C NVARCHAR(255) NOT NULL
         |);""".stripMargin

    val insertCDataSql = s"""|INSERT INTO "$cTableName" (
                             |  SELECT 'c12' AS "C" FROM SYS.DUMMY
                             |  UNION ALL SELECT 'c12' AS "C" FROM SYS.DUMMY
                             |  UNION ALL SELECT 'c3' AS "C" FROM SYS.DUMMY
                             |)""".stripMargin

    Await.result(
      for {
        _ <- TestHelper.executeQuery(createCTableSql)
        _ <- TestHelper.executeQuery(insertCDataSql)
      } yield (),
      5.seconds
    )

  }

  override def cleanUpDb(): Unit = {
    val dropCTableSql = s"""DROP TABLE "$cTableName";"""
    Await.result(TestHelper.executeQuery(dropCTableSql), 5.seconds)
    super.cleanUpDb()
  }
}
