package com.emarsys.rdb.connector.bigquery

import com.emarsys.rdb.connector.bigquery.BigQueryConnector.BigQueryConnectionConfig

trait BigQueryApi {
  val config: BigQueryConnectionConfig

  def queryUrl(projectId: String) = s"https://www.googleapis.com/bigquery/v2/projects/$projectId/queries"
  val tableListUrl = s"https://www.googleapis.com/bigquery/v2/projects/${config.projectId}/datasets/${config.dataset}/tables"
  def fieldListUrl(tableName: String) = s"https://www.googleapis.com/bigquery/v2/projects/${config.projectId}/datasets/${config.dataset}/tables/$tableName"
  def insertIntoUrl(tableName: String) = s"https://www.googleapis.com/bigquery/v2/projects/${config.projectId}/datasets/${config.dataset}/tables/$tableName/insertAll"
  val createTableUrl = s"https://www.googleapis.com/bigquery/v2/projects/${config.projectId}/datasets/${config.dataset}/tables"
  def deleteTableUrl(tableName: String) = s"https://www.googleapis.com/bigquery/v2/projects/${config.projectId}/datasets/${config.dataset}/tables/$tableName"
}
