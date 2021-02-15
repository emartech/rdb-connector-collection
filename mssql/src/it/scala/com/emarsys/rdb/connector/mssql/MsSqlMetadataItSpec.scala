package com.emarsys.rdb.connector.mssql

import com.emarsys.rdb.connector.common.models.TableSchemaDescriptors.FieldModel
import com.emarsys.rdb.connector.mssql.utils.{BaseDbSpec, TestHelper}
import com.emarsys.rdb.connector.test.MetadataItSpec
import scala.concurrent.duration._

import scala.concurrent.Await

class MsSqlMetadataItSpec extends MetadataItSpec with BaseDbSpec {

  val fieldsTable = "fields_table"

  def initDb(): Unit = {
    val createTableSql =
      s"""CREATE TABLE [$tableName] (
         |    PersonID int,
         |    LastName varchar(255),
         |    FirstName varchar(255),
         |    Address varchar(255),
         |    City varchar(255)
         |);""".stripMargin

    val createOtherSchemaSql = s"""CREATE SCHEMA $otherSchema"""

    val createTableOtherSchemaSql =
      s"""CREATE TABLE ${otherSchema}.[$tableNameInOtherSchema] (
         |    ID int
         |);""".stripMargin

    val createViewSql = s"""CREATE VIEW [$viewName] AS
                           |SELECT PersonID, LastName, FirstName
                           |FROM [$tableName];""".stripMargin
    Await.result(
      for {
        _ <- TestHelper.executeQuery(createTableSql)
        _ <- TestHelper.executeQuery(createOtherSchemaSql)
        _ <- TestHelper.executeQuery(createTableOtherSchemaSql)
        _ <- TestHelper.executeQuery(createViewSql)
      } yield (),
      awaitTimeout
    )
  }

  def cleanUpDb(): Unit = {
    val dropViewSql             = s"""DROP VIEW IF EXISTS [$viewName];"""
    val dropSchemaTableSql      = s"""DROP TABLE IF EXISTS [$tableName];"""
    val dropOtherSchemaTableSql = s"""DROP TABLE IF EXISTS ${otherSchema}.[$tableNameInOtherSchema];"""
    val dropOtherSchemaSql      = s"""DROP SCHEMA IF EXISTS [$otherSchema];"""
    val dropFieldsTableSql      = s"""DROP TABLE IF EXISTS [$fieldsTable];"""
    Await.result(
      for {
        _ <- TestHelper.executeQuery(dropViewSql)
        _ <- TestHelper.executeQuery(dropSchemaTableSql)
        _ <- TestHelper.executeQuery(dropOtherSchemaTableSql)
        _ <- TestHelper.executeQuery(dropOtherSchemaSql)
        _ <- TestHelper.executeQuery(dropFieldsTableSql)
      } yield (),
      awaitTimeout
    )
  }

  s"MsSqlMetadataItSpec $uuid" when {
    "#listTablesWithFields" should {
      "list fields with column type" in {
        val createTableSql = s"""CREATE TABLE $fieldsTable (
                            |    PersonID INTEGER PRIMARY KEY,
                            |    LastName varchar(255) NOT NULL,
                            |    FirstName text,
                            |    Address float(5),
                            |    City varchar(15),
                            |    Unsigned BIGINT,
                            |    Date DATE,
                            |    Timestamp TIMESTAMP
                            |);""".stripMargin

        Await.result(TestHelper.executeQuery(createTableSql), 10.seconds)

        val resultE = Await.result(connector.listTablesWithFields(), awaitTimeout)

        resultE shouldBe a[Right[_, _]]
        val result = resultE.value
        result.map(_.name) should contain(fieldsTable)
        result.find(_.name == fieldsTable).get.fields should be(
          Seq(
            FieldModel("PersonID", "int"),
            FieldModel("LastName", "varchar"),
            FieldModel("FirstName", "text"),
            FieldModel("Address", "real"),
            FieldModel("City", "varchar"),
            FieldModel("Unsigned", "bigint"),
            FieldModel("Date", "date"),
            FieldModel("Timestamp", "timestamp")
          )
        )
      }
    }
  }
}
