package com.emarsys.rdb.connector.redshift

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.models.Connector
import com.emarsys.rdb.connector.redshift.utils.TestHelper
import com.emarsys.rdb.connector.test.SelectWithGroupLimitItSpec

import scala.concurrent.Await
import scala.concurrent.duration._

class RedshiftSelectWithGroupLimitItSpec
    extends TestKit(ActorSystem("RedshiftSelectWithGroupLimitItSpec"))
    with SelectWithGroupLimitItSpec {
  import scala.concurrent.ExecutionContext.Implicits.global
  override implicit val materializer: Materializer = ActorMaterializer()
  override val awaitTimeout                        = 20.seconds
  override val queryTimeout                        = 20.seconds

  val connector: Connector =
    Await.result(RedshiftConnector.create(TestHelper.TEST_CONNECTION_CONFIG), awaitTimeout).right.get

  override def afterAll(): Unit = {
    system.terminate()
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
    with SelectWithGroupLimitItSpec {
  import scala.concurrent.ExecutionContext.Implicits.global
  override implicit val materializer: Materializer = ActorMaterializer()

  val schema = "ittestschema"

  val connector: Connector = Await
    .result(
      RedshiftConnector.create(TestHelper.TEST_CONNECTION_CONFIG.copy(connectionParams = s"currentSchema=$schema")),
      awaitTimeout
    )
    .right
    .get
  override def afterAll(): Unit = {
    system.terminate()
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
