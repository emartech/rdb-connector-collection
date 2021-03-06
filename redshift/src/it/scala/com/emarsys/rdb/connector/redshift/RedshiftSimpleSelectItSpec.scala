package com.emarsys.rdb.connector.redshift

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.models.SimpleSelect
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import com.emarsys.rdb.connector.redshift.utils.{SelectDbInitHelper, SelectDbWithSchemaInitHelper, TestHelper}
import com.emarsys.rdb.connector.test._

import scala.concurrent.Await
import scala.concurrent.duration._

class RedshiftSimpleSelectItSpec
    extends TestKit(ActorSystem("RedshiftSimpleSelectItSpec"))
    with SimpleSelectItSpec
    with SelectDbInitHelper {



  override val awaitTimeout = 15.seconds

  override def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }

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

  "list table values with EQUAL" in {
    val simpleSelect =
      SimpleSelect(AllField, TableName(aTableName), where = Some(EqualToValue(FieldName("A2"), Value("3"))))

    val result = getSimpleSelectResult(simpleSelect)

    checkResultWithoutRowOrder(
      result,
      Seq(
        Seq("A1", "A2", "A3"),
        Seq("v3", "3", "1")
      )
    )
  }

}

class RedshiftSimpleSelectWithSchemaItSpec
    extends TestKit(ActorSystem("RedshiftSimpleSelectWithSchemaItSpec"))
    with SimpleSelectItSpec
    with SelectDbWithSchemaInitHelper {



  override val awaitTimeout = 15.seconds

  override def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }

  override def initDb(): Unit = {
    super.initDb()

    val createCTableSql =
      s"""CREATE TABLE "$schema"."$cTableName" (
         |    C varchar(255) NOT NULL
         |);""".stripMargin

    val insertCDataSql =
      s"""INSERT INTO "$schema"."$cTableName" (C) VALUES
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
    val dropCTableSql = s"""DROP TABLE "$schema"."$cTableName";"""
    Await.result(TestHelper.executeQuery(dropCTableSql), 5.seconds)
    super.cleanUpDb()
  }

  "list table values with EQUAL" in {
    val simpleSelect =
      SimpleSelect(AllField, TableName(aTableName), where = Some(EqualToValue(FieldName("A2"), Value("3"))))

    val result = getSimpleSelectResult(simpleSelect)

    checkResultWithoutRowOrder(
      result,
      Seq(
        Seq("A1", "A2", "A3"),
        Seq("v3", "3", "1")
      )
    )
  }

}
