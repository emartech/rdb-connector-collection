package com.emarsys.rbd.connector.bigquery.utils

import scala.concurrent.Await

trait MetaDbInitHelper extends DbInitUtil {

  val tableName: String
  val viewName: String

  lazy val createTableSql =
    s"""
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
       |        "name": "PersonID",
       |        "type": "INTEGER",
       |        "mode": "REQUIRED"
       |      },
       |      {
       |        "name": "LastName",
       |        "type": "STRING"
       |      },
       |      {
       |        "name": "FirstName",
       |        "type": "STRING"
       |      },
       |      {
       |        "name": "Address",
       |        "type": "STRING"
       |      },
       |      {
       |        "name": "City",
       |        "type": "STRING"
       |      }
       |    ]
       |  }
       |}
       """.stripMargin

  lazy val createViewSql =
    s"""
       |{
       |  "friendlyName": "$viewName",
       |  "tableReference": {
       |    "datasetId": "${TestHelper.TEST_CONNECTION_CONFIG.dataset}",
       |    "projectId": "${TestHelper.TEST_CONNECTION_CONFIG.projectId}",
       |    "tableId": "$viewName"
       |  },
       |  "view": {
       |    "query": "SELECT PersonID, LastName, FirstName FROM ${TestHelper.TEST_CONNECTION_CONFIG.dataset}.$tableName"
       |  }
       |}
       """.stripMargin

  def initDb(): Unit = {
    Await.result(for {
      _ <- runRequest(createTable(createTableSql))
      _ <- runRequest(createTable(createViewSql))
    } yield (), timeout.duration)
    sleep()
  }

  def cleanUpDb(): Unit = {
    Await.result(for {
      _ <- runRequest(dropTable(tableName))
      _ <- runRequest(dropTable(viewName))
    } yield (), timeout.duration)
  }
}
