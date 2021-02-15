package com.emarsys.rdb.connector.hana

import com.emarsys.rdb.connector.common.models.TableSchemaDescriptors.{FieldModel, FullTableModel}
import com.emarsys.rdb.connector.hana.utils.{SelectDbInitHelper, TestHelper}
import com.emarsys.rdb.connector.test.MetadataItSpec

import scala.concurrent.duration._
import scala.concurrent.Await

class HanaMetadataItSpec extends MetadataItSpec with SelectDbInitHelper {

  val aTableName: String  = ""
  val bTableName: String  = ""
  val fieldsTable: String = s"fields_table_$uuid"

  override def initDb(): Unit = {
    val createTableSql = s"""CREATE TABLE "$tableName" (
                           |    PersonID int,
                           |    LastName nvarchar,
                           |    FirstName nvarchar,
                           |    Address nvarchar,
                           |    City nvarchar
                           |);""".stripMargin

    val createViewSql = s"""CREATE VIEW "$viewName" AS
                          |SELECT PersonID, LastName, FirstName
                          |FROM "$tableName";""".stripMargin

    val createOtherSchemaSql = s"""CREATE SCHEMA "$otherSchema""""

    val createTableOtherSchemaSql =
      s"""CREATE TABLE "$otherSchema"."$tableNameInOtherSchema" (
         |    ID int
         |);""".stripMargin

    Await.result(
      for {
        _ <- TestHelper.executeQuery(createTableSql)
        _ <- TestHelper.executeQuery(createViewSql)
        _ <- TestHelper.executeQuery(createOtherSchemaSql)
        _ <- TestHelper.executeQuery(createTableOtherSchemaSql)
      } yield (),
      10.seconds
    )
  }

  override def cleanUpDb(): Unit = {
    val dropViewSql             = s"""DROP VIEW "$viewName";"""
    val dropTableSql            = s"""DROP TABLE "$tableName";"""
    val dropView2Sql            = s"""DROP VIEW "${viewName}_2";"""
    val dropTable2Sql           = s"""DROP TABLE "${tableName}_2";"""
    val dropFieldsTableSql      = s"""DROP TABLE "$fieldsTable";"""
    val dropOtherSchemaTableSql = s"""DROP TABLE "$otherSchema"."$tableNameInOtherSchema""""
    val dropOtherSchemaSql      = s"""DROP SCHEMA "$otherSchema""""
    Await.result(
      for {
        _ <- TestHelper.executeQuery(dropViewSql)
        _ <- TestHelper.executeQuery(dropTableSql)
        _ <- TestHelper.executeQuery(dropOtherSchemaTableSql)
        _ <- TestHelper.executeQuery(dropOtherSchemaSql)
        _ <- TestHelper.executeQuery(dropView2Sql).recover { case _ => () }
        _ <- TestHelper.executeQuery(dropTable2Sql).recover { case _ => () }
        _ <- TestHelper.executeQuery(dropFieldsTableSql).recover { case _ => () }
      } yield (),
      10.seconds
    )
  }

  s"HanaMetadataItSpec $uuid" when {

    "#listTablesWithFields" should {
      "list without view after table dropped" in {
        val createTableSql = s"""CREATE TABLE "${tableName}_2" (
                                |    PersonID int,
                                |    LastName nvarchar,
                                |    FirstName nvarchar,
                                |    Address nvarchar,
                                |    City nvarchar
                                |);""".stripMargin

        val createViewSql = s"""CREATE VIEW "${viewName}_2" AS
                               |SELECT PersonID, LastName, FirstName
                               |FROM "${tableName}_2";""".stripMargin

        val dropTableSql = s"""DROP TABLE "${tableName}_2";"""

        Await.result(
          for {
            _ <- TestHelper.executeQuery(createTableSql)
            _ <- TestHelper.executeQuery(createViewSql)
            _ <- TestHelper.executeQuery(dropTableSql)
          } yield (),
          10.seconds
        )

        val tableFields =
          Seq("PersonID", "LastName", "FirstName", "Address", "City").map(_.toLowerCase()).map(FieldModel(_, ""))
        val viewFields = Seq("PersonID", "LastName", "FirstName").map(_.toLowerCase()).map(FieldModel(_, ""))

        val resultE = Await.result(connector.listTablesWithFields(), awaitTimeout)

        resultE shouldBe a[Right[_, _]]
        val result =
          resultE.value.map(x => x.copy(fields = x.fields.map(f => f.copy(name = f.name.toLowerCase, columnType = ""))))
        result should not contain FullTableModel(s"${tableName}_2", false, tableFields)
        result should not contain FullTableModel(s"${viewName}_2", true, viewFields)
      }

      "list fields with column type" in {
        val createTableSql = s"""CREATE TABLE "$fieldsTable" (
                                |    "PersonID" integer PRIMARY KEY,
                                |    "LastName" nvarchar NOT NULL,
                                |    "FirstName" blob,
                                |    "Address" double,
                                |    "City" nvarchar,
                                |    "Unsigned" BIGINT,
                                |    "Date" DATE,
                                |    "Timestamp" TIMESTAMP
                                |);""".stripMargin

        Await.result(TestHelper.executeQuery(createTableSql), 10.seconds)

        val resultE = Await.result(connector.listTablesWithFields(), awaitTimeout)

        resultE shouldBe a[Right[_, _]]
        val result = resultE.value
        result.map(_.name) should contain(fieldsTable)
        result.find(_.name == fieldsTable).get.fields should contain theSameElementsAs Seq(
          FieldModel("PersonID", "INTEGER"),
          FieldModel("LastName", "NVARCHAR"),
          FieldModel("FirstName", "BLOB"),
          FieldModel("Address", "DOUBLE"),
          FieldModel("City", "NVARCHAR"),
          FieldModel("Unsigned", "BIGINT"),
          FieldModel("Date", "DATE"),
          FieldModel("Timestamp", "TIMESTAMP")
        )
      }
    }
  }
}
