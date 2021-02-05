package com.emarsys.rdb.connector.mysql

import com.emarsys.rdb.connector.common.models.TableSchemaDescriptors.{FieldModel, FullTableModel}
import com.emarsys.rdb.connector.mysql.utils.{SelectDbInitHelper, TestHelper}
import com.emarsys.rdb.connector.test.MetadataItSpec

import scala.concurrent.Await
import scala.concurrent.duration._

class MySqlMetadataItSpec extends MetadataItSpec with SelectDbInitHelper {

  val aTableName: String  = ""
  val bTableName: String  = ""
  val fieldsTable: String = "fields_table"

  override def initDb(): Unit = {
    val createTableSql = s"""CREATE TABLE `$tableName` (
                           |    PersonID int,
                           |    LastName varchar(255),
                           |    FirstName varchar(255),
                           |    Address varchar(255),
                           |    City varchar(255)
                           |);""".stripMargin

    val createViewSql = s"""CREATE VIEW `$viewName` AS
                          |SELECT PersonID, LastName, FirstName
                          |FROM `$tableName`;""".stripMargin
    Await.result(for {
      _ <- TestHelper.executeQuery(createTableSql)
      _ <- TestHelper.executeQuery(createViewSql)
    } yield (), 10.seconds)
  }

  override def cleanUpDb(): Unit = {
    val dropViewSql        = s"""DROP VIEW `$viewName`;"""
    val dropTableSql       = s"""DROP TABLE `$tableName`;"""
    val dropViewSql2       = s"""DROP VIEW IF EXISTS `${viewName}_2`;"""
    val dropTableSql2      = s"""DROP TABLE IF EXISTS `${tableName}_2`;"""
    val dropFieldsTableSql = s"""DROP TABLE IF EXISTS `$fieldsTable`;"""
    Await.result(
      for {
        _ <- TestHelper.executeQuery(dropViewSql)
        _ <- TestHelper.executeQuery(dropTableSql)
        _ <- TestHelper.executeQuery(dropViewSql2)
        _ <- TestHelper.executeQuery(dropTableSql2)
        _ <- TestHelper.executeQuery(dropFieldsTableSql)
      } yield (),
      10.seconds
    )
  }

  s"MySqlMetadataItSpec $uuid" when {

    "#listTablesWithFields" should {
      "list without view after table dropped" in {
        val createTableSql = s"""CREATE TABLE `${tableName}_2` (
                                |    PersonID int,
                                |    LastName varchar(255),
                                |    FirstName varchar(255),
                                |    Address varchar(255),
                                |    City varchar(255)
                                |);""".stripMargin

        val createViewSql = s"""CREATE VIEW `${viewName}_2` AS
                               |SELECT PersonID, LastName, FirstName
                               |FROM `${tableName}_2`;""".stripMargin

        val dropTableSql = s"""DROP TABLE `${tableName}_2`;"""

        Await.result(for {
          _ <- TestHelper.executeQuery(createTableSql)
          _ <- TestHelper.executeQuery(createViewSql)
          _ <- TestHelper.executeQuery(dropTableSql)
        } yield (), 10.seconds)

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
        val createTableSql = s"""CREATE TABLE `$fieldsTable` (
                                |    `PersonID` INTEGER PRIMARY KEY,
                                |    `LastName` varchar(255) NOT NULL,
                                |    `FirstName` text,
                                |    `Address` float(5),
                                |    `City` varchar(15),
                                |    `Unsigned` BIGINT UNSIGNED,
                                |    `Date` DATE,
                                |    `Timestamp` TIMESTAMP
                                |);""".stripMargin

        Await.result(TestHelper.executeQuery(createTableSql), 10.seconds)

        val resultE = Await.result(connector.listTablesWithFields(), awaitTimeout)

        resultE shouldBe a[Right[_, _]]
        val result = resultE.value
        result.map(_.name) should contain(fieldsTable)
        result.find(_.name == fieldsTable).get.fields should be(
          Seq(
            FieldModel("PersonID", "int(11)"),
            FieldModel("LastName", "varchar(255)"),
            FieldModel("FirstName", "text"),
            FieldModel("Address", "float"),
            FieldModel("City", "varchar(15)"),
            FieldModel("Unsigned", "bigint(20) unsigned"),
            FieldModel("Date", "date"),
            FieldModel("Timestamp", "timestamp")
          )
        )
      }
    }
  }
}
