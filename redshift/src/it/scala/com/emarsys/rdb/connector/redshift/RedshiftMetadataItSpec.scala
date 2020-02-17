package com.emarsys.rdb.connector.redshift

import com.emarsys.rdb.connector.redshift.utils.{BaseDbSpec, TestHelper}
import com.emarsys.rdb.connector.test.MetadataItSpec

import scala.concurrent.duration._
import scala.concurrent.Await

class RedshiftMetadataItSpec extends MetadataItSpec with BaseDbSpec {

  override val awaitTimeout = 15.seconds

  def initDb(): Unit = {
    val createTableSql =
      s"""CREATE TABLE "$tableName" (
         |    PersonID int,
         |    LastName varchar(255),
         |    FirstName varchar(255),
         |    Address varchar(255),
         |    City varchar(255)
                            |);""".stripMargin

    val createViewSql = s"""CREATE VIEW "$viewName" AS
                           |SELECT PersonID, LastName, FirstName
                           |FROM "$tableName";""".stripMargin
    Await.result(for {
      _ <- TestHelper.executeQuery(createTableSql)
      _ <- TestHelper.executeQuery(createViewSql)
    } yield (), awaitTimeout)
  }

  def cleanUpDb(): Unit = {
    val dropViewSql  = s"""DROP VIEW "$viewName";"""
    val dropTableSql = s"""DROP TABLE "$tableName";"""
    Await.result(for {
      _ <- TestHelper.executeQuery(dropViewSql)
      _ <- TestHelper.executeQuery(dropTableSql)
    } yield (), awaitTimeout)
  }

}
