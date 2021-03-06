package com.emarsys.rdb.connector.mssql

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.emarsys.rdb.connector.mssql.utils.{BaseDbSpec, TestHelper}
import com.emarsys.rdb.connector.test.SearchItSpec

import scala.concurrent.Await

class MsSqlSearchItSpec extends TestKit(ActorSystem("MsSqlSearchItSpec")) with SearchItSpec with BaseDbSpec {



  override def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }

  def initDb(): Unit = {
    val createZTableSql =
      s"""CREATE TABLE [$tableName] (
         |    z1 varchar(255) NOT NULL,
         |    z2 int,
         |    z3 tinyint,
         |    z4 varchar(255),
         |    PRIMARY KEY (z1)
         |);""".stripMargin

    val insertZDataSql =
      s"""INSERT INTO [$tableName] (z1,z2,z3,z4) VALUES
         |  ('r1', 1, 1, 's1'),
         |  ('r2', 2, 0, 's2'),
         |  ('r3', 3, NULL, 's3'),
         |  ('r4', 45, 1, 's4'),
         |  ('r5', 45, 1, 's5')
         |;""".stripMargin

    val addIndex1 =
      s"""CREATE INDEX [${tableName.dropRight(10)}_idx1] ON [$tableName] (z2);"""

    val addIndex2 =
      s"""CREATE INDEX [${tableName.dropRight(5)}_idx2] ON [$tableName] (z3);"""

    Await.result(
      for {
        _ <- TestHelper.executeQuery(createZTableSql)
        _ <- TestHelper.executeQuery(insertZDataSql)
        _ <- TestHelper.executeQuery(addIndex1)
        _ <- TestHelper.executeQuery(addIndex2)
      } yield (),
      awaitTimeout
    )
  }

  def cleanUpDb(): Unit = {
    val dropZTableSql = s"""DROP TABLE [$tableName];"""
    Await.result(for {
      _ <- TestHelper.executeQuery(dropZTableSql)
    } yield (), awaitTimeout)
  }
}
