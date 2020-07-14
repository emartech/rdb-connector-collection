package com.emarsys.rdb.connector.postgresql

import com.emarsys.rdb.connector.common.models.TableSchemaDescriptors.FieldModel
import com.emarsys.rdb.connector.postgresql.utils.{BaseDbSpec, TestHelper}
import com.emarsys.rdb.connector.test.MetadataItSpec

import scala.concurrent.Await
import scala.concurrent.duration._

class PostgreSqlMetadataItSpec extends MetadataItSpec with BaseDbSpec {

  override val awaitTimeout = 15.seconds
  val fieldsTable: String   = "fields_table"

  def initDb(): Unit = {
    val createTableSql =
      s"""CREATE TABLE "$tableName" (
         |    PersonID int,
         |    LastName varchar(255),
         |    FirstName varchar(255),
         |    Address varchar(255),
         |    City varchar(255)
         |);""".stripMargin

    val createOtherSchemaSql = s"""CREATE SCHEMA "$otherSchema""""

    val createTableOtherSchemaSql =
      s"""CREATE TABLE "$otherSchema"."$tableNameInOtherSchema" (
         |    ID int
         |);""".stripMargin

    val createViewSql = s"""CREATE VIEW "$viewName" AS
                           |SELECT PersonID, LastName, FirstName
                           |FROM "$tableName";""".stripMargin
    Await.result(
      for {
        _ <- TestHelper.executeQuery(createTableSql)
        _ <- TestHelper.executeQuery(createOtherSchemaSql)
        _ <- TestHelper.executeQuery(createTableOtherSchemaSql)
        _ <- TestHelper.executeQuery(createViewSql)
      } yield (),
      15.seconds
    )
  }

  def cleanUpDb(): Unit = {
    val dropViewSql        = s"""DROP VIEW "$viewName";"""
    val dropTableSql       = s"""DROP TABLE "$tableName";"""
    val dropOtherTableSql  = s"""DROP TABLE "$otherSchema"."$tableNameInOtherSchema";"""
    val dropOtherSchemaSql = s"""DROP SCHEMA "$otherSchema";"""
    val dropFieldsTableSql = s"""DROP TABLE "$fieldsTable";"""
    Await.result(
      for {
        _ <- TestHelper.executeQuery(dropViewSql)
        _ <- TestHelper.executeQuery(dropTableSql)
        _ <- TestHelper.executeQuery(dropOtherTableSql)
        _ <- TestHelper.executeQuery(dropOtherSchemaSql)
        _ <- TestHelper.executeQuery(dropFieldsTableSql)
      } yield (),
      15.seconds
    )
  }

  s"PostgreSqlMetadataItSpec $uuid" when {

    "#listTablesWithFields" should {
      "list fields with column type" in {
        val createTableSql = s"""CREATE TABLE "$fieldsTable" (
                                |    "PersonID" INTEGER,
                                |    "LastName" varchar(255),
                                |    "FirstName" text,
                                |    "Address" float(5),
                                |    "City" varchar(15),
                                |    "Unsigned" BIGINT,
                                |    "Date" DATE,
                                |    "Timestamp" TIMESTAMP
                                |);""".stripMargin

        Await.result(TestHelper.executeQuery(createTableSql), 10.seconds)

        val resultE = Await.result(connector.listTablesWithFields(), awaitTimeout)

        resultE shouldBe a[Right[_, _]]
        val result = resultE.value
        result.map(_.name) should contain(fieldsTable)
        result.find(_.name == fieldsTable).get.fields should be(
          Seq(
            FieldModel("PersonID", "integer"),
            FieldModel("LastName", "character varying"),
            FieldModel("FirstName", "text"),
            FieldModel("Address", "real"),
            FieldModel("City", "character varying"),
            FieldModel("Unsigned", "bigint"),
            FieldModel("Date", "date"),
            FieldModel("Timestamp", "timestamp without time zone")
          )
        )
      }
    }
  }
}
