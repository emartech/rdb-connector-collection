package com.emarsys.rdb.connector.redshift

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.emarsys.rdb.connector.redshift.utils.{BaseDbSpec, TestHelper}
import com.emarsys.rdb.connector.test.SearchItSpec

import scala.concurrent.Await
import scala.concurrent.duration._

class RedshiftSearchItSpec extends TestKit(ActorSystem("RedshiftSearchItSpec")) with SearchItSpec with BaseDbSpec {



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

    Await.result(for {
      _ <- TestHelper.executeQuery(createZTableSql)
      _ <- TestHelper.executeQuery(insertZDataSql)
    } yield (), 15.seconds)
  }

  def cleanUpDb(): Unit = {
    val dropZTableSql = s"""DROP TABLE "$tableName";"""
    Await.result(for {
      _ <- TestHelper.executeQuery(dropZTableSql)
    } yield (), 15.seconds)
  }
}

class RedshiftSearchWithSchemaItSpec
    extends TestKit(ActorSystem("RedshiftSearchWithSchemaItSpec"))
    with SearchItSpec
    with BaseDbSpec {
  val schema = "ittestschema"

  override val connectionConfig = TestHelper.TEST_CONNECTION_CONFIG.copy(connectionParams = s"currentSchema=$schema")



  override val awaitTimeout = 15.seconds

  override def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }

  def initDb(): Unit = {
    val createZTableSql =
      s"""CREATE TABLE "$schema"."$tableName" (
         |    Z1 varchar(255) NOT NULL,
         |    Z2 int,
         |    Z3 boolean,
         |    Z4 varchar(255),
         |    PRIMARY KEY (Z1)
         |);""".stripMargin

    val insertZDataSql =
      s"""INSERT INTO "$schema"."$tableName" (Z1,Z2,Z3,Z4) VALUES
         |  ('r1', 1, true, 's1'),
         |  ('r2', 2, false, 's2'),
         |  ('r3', 3, NULL, 's3'),
         |  ('r4', 45, true, 's4'),
         |  ('r5', 45, true, 's5')
         |;""".stripMargin

    Await.result(for {
      _ <- TestHelper.executeQuery(createZTableSql)
      _ <- TestHelper.executeQuery(insertZDataSql)
    } yield (), 15.seconds)
  }

  def cleanUpDb(): Unit = {
    val dropZTableSql = s"""DROP TABLE "$schema"."$tableName";"""
    Await.result(for {
      _ <- TestHelper.executeQuery(dropZTableSql)
    } yield (), 15.seconds)
  }
}
