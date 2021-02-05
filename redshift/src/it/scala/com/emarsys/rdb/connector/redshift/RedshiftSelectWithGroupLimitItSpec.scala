package com.emarsys.rdb.connector.redshift

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.emarsys.rdb.connector.redshift.utils.{BaseDbSpec, TestHelper}
import com.emarsys.rdb.connector.test.SelectWithGroupLimitItSpec

import scala.concurrent.Await
import scala.concurrent.duration._

class RedshiftSelectWithGroupLimitItSpec
    extends TestKit(ActorSystem("RedshiftSelectWithGroupLimitItSpec"))
    with SelectWithGroupLimitItSpec
    with BaseDbSpec {

  override val awaitTimeout                        = 20.seconds
  override val queryTimeout                        = 20.seconds

  override def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }

  override def initDb(): Unit = {
    val createTableSql =
      s"""CREATE TABLE $tableName (
         |    ID INT NOT NULL,
         |    NAME varchar(255) NOT NULL,
         |    DATA varchar(255) NOT NULL
         |);""".stripMargin

    val insertDataSql =
      s"""INSERT INTO $tableName (ID, NAME, DATA) VALUES
         |  (1, 'test1', 'data1'),
         |  (1, 'test1', 'data2'),
         |  (1, 'test1', 'data3'),
         |  (1, 'test1', 'data4'),
         |  (2, 'test2', 'data5'),
         |  (2, 'test2', 'data6'),
         |  (2, 'test2', 'data7'),
         |  (2, 'test3', 'data8'),
         |  (3, 'test4', 'data9')
         |;""".stripMargin

    Await.result(for {
      _ <- TestHelper.executeQuery(createTableSql)
      _ <- TestHelper.executeQuery(insertDataSql)
    } yield (), awaitTimeout)
  }

  override def cleanUpDb(): Unit = {
    val dropCTableSql = s"""DROP TABLE $tableName;"""
    Await.result(TestHelper.executeQuery(dropCTableSql), awaitTimeout)
  }
}

class RedshiftSelectWithGroupLimitWithSchemaItSpec
    extends TestKit(ActorSystem("RedshiftSelectWithGroupLimitWithSchemaItSpec"))
    with SelectWithGroupLimitItSpec
    with BaseDbSpec {


  val schema                    = "ittestschema"
  override val connectionConfig = TestHelper.TEST_CONNECTION_CONFIG.copy(connectionParams = s"currentSchema=$schema")

  override def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }

  override def initDb(): Unit = {
    val createTableSql =
      s"""CREATE TABLE "$schema".$tableName (
         |    ID INT NOT NULL,
         |    NAME varchar(255) NOT NULL,
         |    DATA varchar(255) NOT NULL
         |);""".stripMargin

    val insertDataSql =
      s"""INSERT INTO "$schema".$tableName (ID, NAME, DATA) VALUES
         |  (1, 'test1', 'data1'),
         |  (1, 'test1', 'data2'),
         |  (1, 'test1', 'data3'),
         |  (1, 'test1', 'data4'),
         |  (2, 'test2', 'data5'),
         |  (2, 'test2', 'data6'),
         |  (2, 'test2', 'data7'),
         |  (2, 'test3', 'data8'),
         |  (3, 'test4', 'data9')
         |;""".stripMargin

    Await.result(for {
      _ <- TestHelper.executeQuery(createTableSql)
      _ <- TestHelper.executeQuery(insertDataSql)
    } yield (), awaitTimeout)
  }

  override def cleanUpDb(): Unit = {
    val dropCTableSql = s"""DROP TABLE "$schema".$tableName;"""
    Await.result(TestHelper.executeQuery(dropCTableSql), awaitTimeout)
  }
}
