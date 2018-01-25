package com.emarsys.rbd.connector.bigquery.utils

import scala.concurrent.Await

trait SelectDbInitHelper extends DbInitUtil {

  val aTableName: String
  val bTableName: String

  def initDb(): Unit = {
    val createATableSql =
      s"""
         |{
         |  "friendlyName": "$aTableName",
         |  "tableReference": {
         |    "datasetId": "${TestHelper.TEST_CONNECTION_CONFIG.dataset}",
         |    "projectId": "${TestHelper.TEST_CONNECTION_CONFIG.projectId}",
         |    "tableId": "$aTableName"
         |  },
         |  "schema": {
         |    "fields": [
         |      {
         |        "name": "A1",
         |        "type": "STRING",
         |        "mode": "REQUIRED"
         |      },
         |      {
         |        "name": "A2",
         |        "type": "INTEGER"
         |      },
         |      {
         |        "name": "A3",
         |        "type": "BOOL"
         |      }
         |    ]
         |  }
         |}
       """.stripMargin

    val createBTableSql =
      s"""
         |{
         |  "friendlyName": "$bTableName",
         |  "tableReference": {
         |    "datasetId": "${TestHelper.TEST_CONNECTION_CONFIG.dataset}",
         |    "projectId": "${TestHelper.TEST_CONNECTION_CONFIG.projectId}",
         |    "tableId": "$bTableName"
         |  },
         |  "schema": {
         |    "fields": [
         |      {
         |        "name": "B1",
         |        "type": "STRING",
         |        "mode": "REQUIRED"
         |      },
         |      {
         |        "name": "B2",
         |        "type": "STRING",
         |        "mode": "REQUIRED"
         |      },
         |      {
         |        "name": "B3",
         |        "type": "STRING",
         |        "mode": "REQUIRED"
         |      },
         |      {
         |        "name": "B4",
         |        "type": "STRING"
         |      }
         |    ]
         |  }
         |}
       """.stripMargin

    val insertADataSql =
      """
        |{
        |  rows: [
        |    {
        |      "json": {
        |        "A1": "v1",
        |        "A2": 1,
        |        "A3": true
        |      }
        |    },
        |    {
        |      "json": {
        |        "A1": "v2",
        |        "A2": 2,
        |        "A3": false
        |      }
        |    },
        |    {
        |      "json": {
        |        "A1": "v3",
        |        "A2": 3,
        |        "A3": true
        |      }
        |    },
        |    {
        |      "json": {
        |        "A1": "v4",
        |        "A2": -4,
        |        "A3": false
        |      }
        |    },
        |    {
        |      "json": {
        |        "A1": "v5",
        |        "A2": null,
        |        "A3": false
        |      }
        |    },
        |    {
        |      "json": {
        |        "A1": "v6",
        |        "A2": 6,
        |        "A3": null
        |      }
        |    },
        |    {
        |      "json": {
        |        "A1": "v7",
        |        "A2": null,
        |        "A3": null
        |      }
        |    }
        |  ]
        |}
      """.stripMargin

    val insertBDataSql =
      raw"""
           |{
           |  "rows": [
           |    {
           |      "json": {
           |        "B1": "b,1",
           |        "B2": "b.1",
           |        "B3": "b:1",
           |        "B4": "b\"1"
           |      }
           |    },
           |    {
           |      "json": {
           |        "B1": "b;2",
           |        "B2": "b\\2",
           |        "B3": "b\'2",
           |        "B4": "b=2"
           |      }
           |    },
           |    {
           |      "json": {
           |        "B1": "b!3",
           |        "B2": "b@3",
           |        "B3": "b#3",
           |        "B4": null
           |      }
           |    },
           |    {
           |      "json": {
           |        "B1": "b$$4",
           |        "B2": "b%4",
           |        "B3": "b 4",
           |        "B4": null
           |      }
           |    }
           |  ]
           |}
       """.stripMargin

    Await.result(for {
      _ <- runRequest(createTable(createATableSql))
      _ <- runRequest(createTable(createBTableSql))
      _ <- runRequest(insertInto(insertADataSql, aTableName))
      _ <- runRequest(insertInto(insertBDataSql, bTableName))
    } yield (), timeout.duration)
    sleep()
  }

  def cleanUpDb(): Unit = {
    Await.result(for {
      _ <- runRequest(dropTable(aTableName))
      _ <- runRequest(dropTable(bTableName))
    } yield (), timeout.duration)
  }
}
