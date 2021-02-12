package com.emarsys.rdb.connector.hana.utils

import scala.concurrent.Await
import scala.concurrent.duration._

trait SelectDbInitHelper extends BaseDbSpec {

  val aTableName: String
  val bTableName: String

  def initDb(): Unit = {
    val createATableSql =
      s"""CREATE TABLE "$aTableName" (
         |    A1 NVARCHAR(255) NOT NULL,
         |    A2 INTEGER,
         |    A3 BOOLEAN,
         |    PRIMARY KEY (A1)
         |);""".stripMargin

    val createBTableSql =
      s"""CREATE TABLE "$bTableName" (
         |    B1 NVARCHAR(255) NOT NULL,
         |    B2 NVARCHAR(255) NOT NULL,
         |    B3 NVARCHAR(255) NOT NULL,
         |    B4 NVARCHAR(255)
         |);""".stripMargin

    val insertADataSql = s"""|INSERT INTO "$aTableName" (
                             |  SELECT 'v1' AS "A1", 1 AS "A2", TRUE AS "A3" FROM SYS.DUMMY
                             |  UNION ALL SELECT 'v2' AS "A1", 2 AS "A2", FALSE AS "A3" FROM SYS.DUMMY
                             |  UNION ALL SELECT 'v3' AS "A1", 3 AS "A2", TRUE AS "A3" FROM SYS.DUMMY
                             |  UNION ALL SELECT 'v4' AS "A1", -4 AS "A2", FALSE AS "A3" FROM SYS.DUMMY
                             |  UNION ALL SELECT 'v5' AS "A1", NULL AS "A2", FALSE AS "A3" FROM SYS.DUMMY
                             |  UNION ALL SELECT 'v6' AS "A1", 6 AS "A2", NULL AS "A3" FROM SYS.DUMMY
                             |  UNION ALL SELECT 'v7' AS "A1", NULL AS "A2", NULL AS "A3" FROM SYS.DUMMY
                             |)""".stripMargin

    val insertBDataSql = s"""|INSERT INTO "$bTableName" (
                             |  SELECT 'b,1' AS "B1", 'b.1' AS "B2", 'b:1' AS "B3", 'b"1' AS "B4" FROM SYS.DUMMY
                             |  UNION ALL SELECT 'b;2' AS "B1", 'b\\2' AS "B2", 'b''2' AS "B3", 'b=2' AS "B4" FROM SYS.DUMMY
                             |  UNION ALL SELECT 'b!3' AS "B1", 'b@3' AS "B2", 'b#3' AS "B3", NULL AS "B4" FROM SYS.DUMMY
                             |  UNION ALL SELECT 'b$$4' AS "B1", 'b%4' AS "B2", 'b 4' AS "B3", NULL AS "B4" FROM SYS.DUMMY
                             |)""".stripMargin

    val truncateExplainPlanTableSql = "DELETE FROM explain_plan_table"

    Await.result(
      for {
        _ <- TestHelper.executeQuery(createATableSql)
        _ <- TestHelper.executeQuery(createBTableSql)
        _ <- TestHelper.executeQuery(insertADataSql)
        _ <- TestHelper.executeQuery(insertBDataSql)
        _ <- TestHelper.executeQuery(truncateExplainPlanTableSql)
      } yield (),
      20.seconds
    )
  }

  def cleanUpDb(): Unit = {
    val dropATableSql = s"""DROP TABLE "$aTableName";"""
    val dropBTableSql = s"""DROP TABLE "$bTableName";"""
    Await.result(
      for {
        _ <- TestHelper.executeQuery(dropATableSql)
        _ <- TestHelper.executeQuery(dropBTableSql)
      } yield (),
      10.seconds
    )
  }
}
