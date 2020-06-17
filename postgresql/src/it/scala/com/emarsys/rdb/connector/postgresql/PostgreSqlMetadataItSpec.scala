package com.emarsys.rdb.connector.postgresql

import com.emarsys.rdb.connector.postgresql.utils.{BaseDbSpec, TestHelper}
import com.emarsys.rdb.connector.test.MetadataItSpec

import scala.concurrent.Await
import scala.concurrent.duration._

class PostgreSqlMetadataItSpec extends MetadataItSpec with BaseDbSpec {

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

    val createOtherSchemaSql = s"""CREATE SCHEMA $otherSchema"""

    val createTableOtherSchemaSql =
      s"""CREATE TABLE ${otherSchema}."$tableNameInOtherSchema" (
         |    ID int
         |);""".stripMargin

    val createViewSql = s"""CREATE VIEW "$viewName" AS
                           |SELECT PersonID, LastName, FirstName
                           |FROM "$tableName";""".stripMargin
    Await.result(for {
      _ <- TestHelper.executeQuery(createTableSql)
      _ <- TestHelper.executeQuery(createOtherSchemaSql)
      _ <- TestHelper.executeQuery(createTableOtherSchemaSql)
      _ <- TestHelper.executeQuery(createViewSql)
    } yield (), 15.seconds)
  }

  def cleanUpDb(): Unit = {
    val dropViewSql  = s"""DROP VIEW "$viewName";"""
    val dropTableSql = s"""DROP TABLE "$tableName";"""
    val dropOtherTableSql = s"""DROP TABLE "$tableNameInOtherSchema";"""
    val dropOtherSchemaSql = s"""DROP SCHEMA "$otherSchema";"""
    Await.result(for {
      _ <- TestHelper.executeQuery(dropViewSql)
      _ <- TestHelper.executeQuery(dropTableSql)
      _ <- TestHelper.executeQuery(dropOtherTableSql)
      _ <- TestHelper.executeQuery(dropOtherSchemaSql)
    } yield (), 15.seconds)
  }

}
