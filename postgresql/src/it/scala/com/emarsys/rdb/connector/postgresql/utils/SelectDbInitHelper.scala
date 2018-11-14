package com.emarsys.rdb.connector.postgresql.utils

import com.emarsys.rdb.connector.common.models.Connector
import com.emarsys.rdb.connector.postgresql.PostgreSqlConnector
import slick.util.AsyncExecutor

import concurrent.duration._
import scala.concurrent.Await

trait SelectDbInitHelper {

  import scala.concurrent.ExecutionContext.Implicits.global

  val aTableName: String
  val bTableName: String

  val connector: Connector =
    Await.result(PostgreSqlConnector(TestHelper.TEST_CONNECTION_CONFIG)(AsyncExecutor.default()), 5.seconds).right.get

  def initDb(): Unit = {
    val createATableSql =
      s"""CREATE TABLE "$aTableName" (
         |    A1 varchar(255) NOT NULL,
         |    A2 int,
         |    A3 boolean,
         |    PRIMARY KEY (A1)
         |);""".stripMargin

    val createBTableSql =
      s"""CREATE TABLE "$bTableName" (
         |    B1 varchar(255) NOT NULL,
         |    B2 varchar(255) NOT NULL,
         |    B3 varchar(255) NOT NULL,
         |    B4 varchar(255)
         |);""".stripMargin

    val insertADataSql =
      s"""INSERT INTO "$aTableName" (a1,a2,a3) VALUES
         |('v1', 1, true),
         |('v2', 2, false),
         |('v3', 3, true),
         |('v4', -4, false),
         |('v5', NULL, false),
         |('v6', 6, NULL),
         |('v7', NULL, NULL)
         |;""".stripMargin

    val insertBDataSql =
      s"""INSERT INTO "$bTableName" (b1,b2,b3,b4) VALUES
         |('b,1', 'b.1', 'b:1', 'b"1'),
         |('b;2', 'b\\2', 'b''2', 'b=2'),
         |('b!3', 'b@3', 'b#3', NULL),
         |('b$$4', 'b%4', 'b 4', NULL)
         |;""".stripMargin

    val addIndex1 =
      s"""CREATE INDEX "${aTableName.dropRight(5)}_idx1" ON "$aTableName" (A3);"""

    val addIndex2 =
      s"""CREATE INDEX "${aTableName.dropRight(5)}_idx2" ON "$aTableName" (A2, A3);"""

    Await.result(
      for {
        _ <- TestHelper.executeQuery(createATableSql)
        _ <- TestHelper.executeQuery(createBTableSql)
        _ <- TestHelper.executeQuery(insertADataSql)
        _ <- TestHelper.executeQuery(insertBDataSql)
        _ <- TestHelper.executeQuery(addIndex1)
        _ <- TestHelper.executeQuery(addIndex2)
      } yield (),
      5.seconds
    )
  }

  def cleanUpDb(): Unit = {
    val dropATableSql = s"""DROP TABLE "$aTableName";"""
    val dropBTableSql = s"""DROP TABLE "$bTableName";"""
    Await.result(for {
      _ <- TestHelper.executeQuery(dropATableSql)
      _ <- TestHelper.executeQuery(dropBTableSql)
    } yield (), 15.seconds)
  }
}
