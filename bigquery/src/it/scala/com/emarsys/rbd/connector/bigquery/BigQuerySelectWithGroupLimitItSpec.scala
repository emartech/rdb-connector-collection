package com.emarsys.rbd.connector.bigquery

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import akka.util.Timeout
import com.emarsys.rbd.connector.bigquery.utils.{DbInitUtil, TestHelper}
import com.emarsys.rdb.connector.test.SelectWithGroupLimitItSpec

import scala.concurrent.Await
import scala.concurrent.duration._

class BigQuerySelectWithGroupLimitItSpec
    extends TestKit(ActorSystem("BigQuerySelectWithGroupLimitItSpec"))
    with SelectWithGroupLimitItSpec
    with DbInitUtil {
  override implicit val sys: ActorSystem                = system
  override implicit val materializer: ActorMaterializer = ActorMaterializer()
  override implicit val timeout: Timeout                = 20.seconds
  override val awaitTimeout                             = 20.seconds
  override val queryTimeout                             = 20.seconds

  override def afterAll(): Unit = {
    super.afterAll()
    shutdown()
  }

  override def initDb(): Unit = {

    val createTableSql = s"""
       |{
       |  "friendlyName": "$tableName",
       |  "tableReference": {
       |    "datasetId": "${TestHelper.TEST_CONNECTION_CONFIG.dataset}",
       |    "projectId": "${TestHelper.TEST_CONNECTION_CONFIG.projectId}",
       |    "tableId": "$tableName"
       |  },
       |  "schema": {
       |    "fields": [
       |      {
       |        "name": "ID",
       |        "type": "INTEGER",
       |        "mode": "REQUIRED"
       |      },
       |      {
       |        "name": "NAME",
       |        "type": "STRING"
       |      },
       |      {
       |        "name": "DATA",
       |        "type": "STRING"
       |      }
       |    ]
       |  }
       |}
       """.stripMargin

    val insertDataSql =
      s"""
         |{
         |  rows: [
         |  { "json": {"ID": 1, "NAME": "test1", "DATA": "data1"}},
         |  { "json": {"ID": 1, "NAME": "test1", "DATA": "data2"}},
         |  { "json": {"ID": 1, "NAME": "test1", "DATA": "data3"}},
         |  { "json": {"ID": 1, "NAME": "test1", "DATA": "data4"}},
         |  { "json": {"ID": 2, "NAME": "test2", "DATA": "data5"}},
         |  { "json": {"ID": 2, "NAME": "test2", "DATA": "data6"}},
         |  { "json": {"ID": 2, "NAME": "test2", "DATA": "data7"}},
         |  { "json": {"ID": 2, "NAME": "test3", "DATA": "data8"}},
         |  { "json": {"ID": 3, "NAME": "test4", "DATA": "data9"}}
         |  ]
         |}""".stripMargin

    Await.result(for {
      _ <- runRequest(createTable(createTableSql))
      _ <- runRequest(insertInto(insertDataSql, tableName))
    } yield (), timeout.duration)
    sleep()
  }

  override def cleanUpDb(): Unit = {
    Await.result(runRequest(dropTable(tableName)), timeout.duration)
    sleep()
  }
}
