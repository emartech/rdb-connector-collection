package com.emarsys.rdb.connector.mssql

import com.emarsys.rdb.connector.mssql.utils.{BaseDbSpec, TestHelper}
import com.emarsys.rdb.connector.test.MetadataItSpec

import scala.concurrent.Await

class MsSqlMetadataItSpec extends MetadataItSpec with BaseDbSpec {

  def initDb(): Unit = {
    val createTableSql =
      s"""CREATE TABLE [$tableName] (
         |    PersonID int,
         |    LastName varchar(255),
         |    FirstName varchar(255),
         |    Address varchar(255),
         |    City varchar(255)
         |);""".stripMargin

    val createViewSql = s"""CREATE VIEW [$viewName] AS
                           |SELECT PersonID, LastName, FirstName
                           |FROM [$tableName];""".stripMargin
    Await.result(for {
      _ <- TestHelper.executeQuery(createTableSql)
      _ <- TestHelper.executeQuery(createViewSql)
    } yield (), awaitTimeout)
  }

  def cleanUpDb(): Unit = {
    val dropViewSql   = s"""DROP VIEW [$viewName];"""
    val dropTableSql  = s"""DROP TABLE [$tableName];"""
    val dropViewSql2  = s"""IF OBJECT_ID('${viewName}_2', 'V') IS NOT NULL DROP VIEW [${viewName}_2];"""
    val dropTableSql2 = s"""IF OBJECT_ID('${tableName}_2', 'U') IS NOT NULL DROP TABLE [${tableName}_2];"""
    Await.result(
      for {
        _ <- TestHelper.executeQuery(dropViewSql)
        _ <- TestHelper.executeQuery(dropTableSql)
        _ <- TestHelper.executeQuery(dropViewSql2)
        _ <- TestHelper.executeQuery(dropTableSql2)
      } yield (),
      awaitTimeout
    )
  }
}
