package com.emarsys.rdb.connector.mssql

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.mssql.utils.{SelectDbInitHelper, TestHelper}
import com.emarsys.rdb.connector.test.SimpleSelectItSpec

import scala.concurrent.Await
import scala.concurrent.duration._

class MsSqlSimpleSelectItSpec
    extends TestKit(ActorSystem("MsSqlSimpleSelectItSpec"))
    with SimpleSelectItSpec
    with SelectDbInitHelper {

  override val awaitTimeout: FiniteDuration = 30.seconds
  override implicit val materializer: Materializer = ActorMaterializer()

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  override def initDb(): Unit = {
    super.initDb()

    val createCTableSql =
      s"""CREATE TABLE [$cTableName] (
         |    C varchar(255) NOT NULL
         |);""".stripMargin

    val insertCDataSql =
      s"""INSERT INTO [$cTableName] (C) VALUES
         |('c12'),
         |('c12'),
         |('c3')
         |;""".stripMargin

    Await.result(for {
      _ <- TestHelper.executeQuery(createCTableSql)
      _ <- TestHelper.executeQuery(insertCDataSql)
    } yield (), awaitTimeout)
  }

  override def cleanUpDb(): Unit = {
    val dropCTableSql = s"""DROP TABLE [$cTableName];"""
    Await.result(TestHelper.executeQuery(dropCTableSql), awaitTimeout)
    super.cleanUpDb()
  }
}
