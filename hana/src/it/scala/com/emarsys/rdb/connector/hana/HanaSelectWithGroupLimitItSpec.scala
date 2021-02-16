package com.emarsys.rdb.connector.hana

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.emarsys.rdb.connector.hana.utils.{BaseDbSpec, TestHelper}
import com.emarsys.rdb.connector.test.SelectWithGroupLimitItSpec

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class HanaSelectWithGroupLimitItSpec
    extends TestKit(ActorSystem("HanaSelectWithGroupLimitItSpec"))
    with SelectWithGroupLimitItSpec
    with BaseDbSpec {

  override def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }

  override def initDb(): Unit = {
    val createTableSql =
      s"""CREATE TABLE "$tableName" (
         |    ID INT NOT NULL,
         |    NAME varchar(255) NOT NULL,
         |    DATA varchar(255) NOT NULL
         |);""".stripMargin

    val insertDataSql =
      s"""INSERT INTO "$tableName" (ID, NAME, DATA) (
         |  SELECT 1 AS "ID", 'test1' AS "NAME", 'data1' AS "DATA" FROM SYS.DUMMY
         |  UNION ALL SELECT 1 AS "ID", 'test1' AS "NAME", 'data2' AS "DATA" FROM SYS.DUMMY
         |  UNION ALL SELECT 1 AS "ID", 'test1' AS "NAME", 'data3' AS "DATA" FROM SYS.DUMMY
         |  UNION ALL SELECT 1 AS "ID", 'test1' AS "NAME", 'data4' AS "DATA" FROM SYS.DUMMY
         |  UNION ALL SELECT 2 AS "ID", 'test2' AS "NAME", 'data5' AS "DATA" FROM SYS.DUMMY
         |  UNION ALL SELECT 2 AS "ID", 'test2' AS "NAME", 'data6' AS "DATA" FROM SYS.DUMMY
         |  UNION ALL SELECT 2 AS "ID", 'test2' AS "NAME", 'data7' AS "DATA" FROM SYS.DUMMY
         |  UNION ALL SELECT 2 AS "ID", 'test3' AS "NAME", 'data8' AS "DATA" FROM SYS.DUMMY
         |  UNION ALL SELECT 3 AS "ID", 'test4' AS "NAME", 'data9' AS "DATA" FROM SYS.DUMMY
         |);""".stripMargin

    Await.result(
      for {
        _ <- TestHelper.executeQuery(createTableSql)
        _ <- TestHelper.executeQuery(insertDataSql)
      } yield (),
      10.seconds
    )
  }

  override def cleanUpDb(): Unit = {
    val dropCTableSql = s"""DROP TABLE "$tableName";"""
    Await.result(TestHelper.executeQuery(dropCTableSql), 5.seconds)
  }
}
