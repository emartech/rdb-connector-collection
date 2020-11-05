package com.emarsys.rbd.connector.bigquery

import akka.actor.ActorSystem
import akka.testkit.TestKit
import akka.util.Timeout
import com.emarsys.rbd.connector.bigquery.utils.{SelectDbInitHelper, TestHelper}
import com.emarsys.rdb.connector.common.models.SimpleSelect
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import com.emarsys.rdb.connector.test._

import scala.concurrent.Await
import scala.concurrent.duration._

class BigQuerySimpleSelectItSpec
    extends TestKit(ActorSystem("BigQuerySimpleSelectItSpec"))
    with SimpleSelectItSpec
    with SelectDbInitHelper {

  implicit override val sys: ActorSystem                = system

  implicit override val timeout: Timeout                = 10.seconds
  override val awaitTimeout                             = timeout.duration
  override val queryTimeout                             = timeout.duration

  override def beforeAll(): Unit = {
    super.beforeAll()
    initCTable()
  }

  override def afterAll(): Unit = {
    Await.result(runRequest(dropTable(cTableName)), timeout.duration)
    super.afterAll()
    shutdown()
  }

  "list table values with EQUAL on booleans with case-insensitive true/false values" in {
    val simpleSelect =
      SimpleSelect(AllField, TableName(aTableName), where = Some(EqualToValue(FieldName("A3"), Value("trUE"))))

    val result = getSimpleSelectResult(simpleSelect)

    checkResultWithoutRowOrder(
      result,
      Seq(
        Seq("A1", "A2", "A3"),
        Seq("v1", "1", "1"),
        Seq("v3", "3", "1")
      )
    )
  }

  private def initCTable(): Unit = {
    val createCTableSql =
      s"""{
         |  "friendlyName": "$cTableName",
         |  "tableReference": {
         |    "datasetId": "${TestHelper.TEST_CONNECTION_CONFIG.dataset}",
         |    "projectId": "${TestHelper.TEST_CONNECTION_CONFIG.projectId}",
         |    "tableId": "$cTableName"
         |  },
         |  "schema": {
         |    "fields": [
         |      {
         |        "name": "C",
         |        "type": "STRING",
         |        "mode": "REQUIRED"
         |      }
         |    ]
         |  }
         |}
       """.stripMargin

    val insertCDataSql =
      """
        |{
        |  rows: [
        |    {
        |      "json": {
        |        "C": "c12",
        |      }
        |    },
        |    {
        |      "json": {
        |        "C": "c12",
        |      }
        |    },
        |    {
        |      "json": {
        |        "C": "c3",
        |      }
        |    }
        |  ]
        |}
      """.stripMargin

    Await.result(for {
      _ <- runRequest(createTable(createCTableSql))
      _ <- runRequest(insertInto(insertCDataSql, cTableName))
    } yield (), timeout.duration)
    sleep()
  }
}
