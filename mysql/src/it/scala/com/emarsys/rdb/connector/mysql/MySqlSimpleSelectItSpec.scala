package com.emarsys.rdb.connector.mysql

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorName}
import com.emarsys.rdb.connector.common.models.Errors.ErrorCategory.FatalQueryExecution
import com.emarsys.rdb.connector.common.models.SimpleSelect
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import com.emarsys.rdb.connector.mysql.utils.{SelectDbInitHelper, TestHelper}
import com.emarsys.rdb.connector.test.CustomMatchers.beDatabaseErrorEqualWithoutCause
import com.emarsys.rdb.connector.test.SimpleSelectItSpec

import scala.concurrent.Await
import scala.concurrent.duration._

class MySqlSimpleSelectItSpec
    extends TestKit(ActorSystem("MySqlSimpleSelectItSpec"))
    with SimpleSelectItSpec
    with SelectDbInitHelper {

  implicit override val materializer: Materializer = ActorMaterializer()

  override def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }

  override def initDb(): Unit = {
    super.initDb()

    val createCTableSql =
      s"""CREATE TABLE `$cTableName` (
         |    C varchar(255) NOT NULL
         |);""".stripMargin

    val insertCDataSql =
      s"""INSERT INTO `$cTableName` (C) VALUES
         |('c12'),
         |('c12'),
         |('c3')
         |;""".stripMargin

    Await.result(for {
      _ <- TestHelper.executeQuery(createCTableSql)
      _ <- TestHelper.executeQuery(insertCDataSql)
    } yield (), 10.seconds)
  }

  override def cleanUpDb(): Unit = {
    val dropCTableSql = s"""DROP TABLE `$cTableName`;"""
    Await.result(TestHelper.executeQuery(dropCTableSql), 5.seconds)
    super.cleanUpDb()
  }

  "#simpleSelect" should {

    "return SqlSyntaxError when there is a syntax error in the query" in {
      val message  = "Unknown column 'nope' in 'field list'"
      val expected = DatabaseError(FatalQueryExecution, ErrorName.SqlSyntaxError, message, None, None)
      val select   = SimpleSelect(SpecificFields(Seq(FieldName("nope"))), TableName(aTableName))
      val error    = the[DatabaseError] thrownBy getSimpleSelectResult(select)

      error should beDatabaseErrorEqualWithoutCause(expected)
    }

  }

}
