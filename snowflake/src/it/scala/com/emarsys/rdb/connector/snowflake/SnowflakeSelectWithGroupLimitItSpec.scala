package com.emarsys.rdb.connector.snowflake

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.emarsys.rdb.connector.snowflake.utils.{BaseDbSpec, TestHelper}
import com.emarsys.rdb.connector.test.SelectWithGroupLimitItSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Await
import scala.concurrent.duration._

class SnowflakeSelectWithGroupLimitItSpec
    extends TestKit(ActorSystem("SnowflakeSimpleSelectItSpec"))
    with SelectWithGroupLimitItSpec
    with BaseDbSpec
    with Matchers {



  override def initDb(): Unit = {
    val createTableSql = s"""CREATE TABLE "$tableName"(ID NUMBER, NAME VARCHAR, DATA VARCHAR)"""
    val insertDataSql =
      s"""
        |INSERT INTO "$tableName" VALUES
        |  (1, 'test1', 'data1'),
        |  (1, 'test1', 'data2'),
        |  (1, 'test1', 'data3'),
        |  (1, 'test1', 'data4'),
        |  (2, 'test2', 'data5'),
        |  (2, 'test2', 'data6'),
        |  (2, 'test2', 'data7'),
        |  (2, 'test3', 'data8'),
        |  (3, 'test4', 'data9')
        |""".stripMargin
    Await.result(for {
      _ <- TestHelper.executeQuery(createTableSql)
      _ <- TestHelper.executeQuery(insertDataSql)
    } yield (), 5.seconds)

  }

  override def cleanUpDb(): Unit = {
    val dropTableSql = s"""DROP TABLE "$tableName""""
    Await.result(TestHelper.executeQuery(dropTableSql), 5.seconds)
  }
}
