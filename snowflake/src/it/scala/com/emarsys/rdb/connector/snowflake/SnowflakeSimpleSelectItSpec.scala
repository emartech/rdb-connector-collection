package com.emarsys.rdb.connector.snowflake

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.models.Errors.ErrorCategory.FatalQueryExecution
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorName}
import com.emarsys.rdb.connector.common.models.SimpleSelect
import com.emarsys.rdb.connector.common.models.SimpleSelect.{AllField, FieldName, SpecificFields, TableName}
import com.emarsys.rdb.connector.snowflake.utils.{SelectDbInitHelper, TestHelper}
import com.emarsys.rdb.connector.test.CustomMatchers.beDatabaseErrorEqualWithoutCause
import com.emarsys.rdb.connector.test.SimpleSelectItSpec
import org.scalatest.Matchers

import scala.concurrent.Await
import scala.concurrent.duration._


class SnowflakeSimpleSelectItSpec
  extends TestKit(ActorSystem("SnowflakeSimpleSelectItSpec"))
  with SimpleSelectItSpec
  with SelectDbInitHelper
  with Matchers {

  implicit override val materializer: Materializer = ActorMaterializer()

  override val booleanValue0 = "FALSE"
  override val booleanValue1 = "TRUE"

  override def initDb(): Unit = {
    super.initDb()

    val createCTableSql =
      s"""CREATE TABLE "$cTableName" (
         |    C varchar(255) NOT NULL
         |);""".stripMargin

    val insertCDataSql =
      s"""INSERT INTO "$cTableName" (C) VALUES
         |('c12'),
         |('c12'),
         |('c3')
         |;""".stripMargin

    Await.result(for {
      _ <- TestHelper.executeQuery(createCTableSql)
      _ <- TestHelper.executeQuery(insertCDataSql)
    } yield (), 5.seconds)
  }

  override def cleanUpDb(): Unit = {
    val dropCTableSql = s"""DROP TABLE "$cTableName";"""
    Await.result(TestHelper.executeQuery(dropCTableSql), 5.seconds)
    super.cleanUpDb()
  }

  "#simpleSelect" should {

    "return SqlSyntaxError when there is a syntax error in the query" in {
      val message  = "SQL compilation error: error line 1 at position 7\ninvalid identifier '\"nope\"'"
      val expected = DatabaseError(FatalQueryExecution, ErrorName.SqlSyntaxError, message, None, None)
      val select   = SimpleSelect(SpecificFields(Seq(FieldName("nope"))), TableName(aTableName))
      val error    = the[DatabaseError] thrownBy getSimpleSelectResult(select)

      error should beDatabaseErrorEqualWithoutCause(expected)
    }

    "return TableNotFound when the specified table is not found" in {
      val message  = "SQL compilation error:\nObject '\"nope\"' does not exist or not authorized."
      val expected = DatabaseError(FatalQueryExecution, ErrorName.TableNotFound, message, None, None)
      val select   = SimpleSelect(AllField, TableName("nope"))
      val error    = the[DatabaseError] thrownBy getSimpleSelectResult(select)

      error should beDatabaseErrorEqualWithoutCause(expected)
    }

  }
}
