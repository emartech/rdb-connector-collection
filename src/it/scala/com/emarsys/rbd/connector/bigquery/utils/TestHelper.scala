package com.emarsys.rbd.connector.bigquery.utils

import com.emarsys.rdb.connector.bigquery.BigQueryConnector.BigQueryConnectionConfig

object TestHelper {
  import com.typesafe.config.ConfigFactory
  lazy val config = ConfigFactory.load()

  lazy val TEST_CONNECTION_CONFIG = BigQueryConnectionConfig(
    projectId = config.getString("dbconfig.projectId"),
    dataset = config.getString("dbconfig.dataset"),
    clientEmail = config.getString("dbconfig.clientEmail"),
    privateKey = config.getString("dbconfig.privateKey").replace("\\n","\n")
  )
}
