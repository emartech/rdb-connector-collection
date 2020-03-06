package com.emarsys.rdb.connector.postgresql

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.postgresql.utils.{BaseDbSpec, TestHelper}
import com.emarsys.rdb.connector.test.SearchItSpec

import scala.concurrent.Await
import scala.concurrent.duration._

class PostgreSqlSearchItSpec extends TestKit(ActorSystem("PostgreSqlSearchItSpec")) with SearchItSpec with BaseDbSpec {

  implicit override val materializer: Materializer = ActorMaterializer()

  override val awaitTimeout = 15.seconds

  override def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }

  def initDb(): Unit = {
    val createZTableSql =
      s"""CREATE TABLE "$tableName" (
         |    Z1 varchar(255) NOT NULL,
         |    Z2 int,
         |    Z3 boolean,
         |    Z4 varchar(255),
         |    PRIMARY KEY (Z1)
         |);""".stripMargin

    val insertZDataSql =
      s"""INSERT INTO "$tableName" (Z1,Z2,Z3,Z4) VALUES
         |  ('r1', 1, true, 's1'),
         |  ('r2', 2, false, 's2'),
         |  ('r3', 3, NULL, 's3'),
         |  ('r4', 45, true, 's4'),
         |  ('r5', 45, true, 's5')
         |;""".stripMargin

    val addIndex1 =
      s"""CREATE INDEX "${tableName.dropRight(5)}_idx1" ON "$tableName" (Z2);"""

    val addIndex2 =
      s"""CREATE INDEX "${tableName.dropRight(5)}_idx2" ON "$tableName" (Z3);"""

    Await.result(
      for {
        _ <- TestHelper.executeQuery(createZTableSql)
        _ <- TestHelper.executeQuery(insertZDataSql)
        _ <- TestHelper.executeQuery(addIndex1)
        _ <- TestHelper.executeQuery(addIndex2)
      } yield (),
      5.seconds
    )
  }

  def cleanUpDb(): Unit = {
    val dropZTableSql = s"""DROP TABLE "$tableName";"""
    Await.result(for {
      _ <- TestHelper.executeQuery(dropZTableSql)
    } yield (), 15.seconds)
  }
}
