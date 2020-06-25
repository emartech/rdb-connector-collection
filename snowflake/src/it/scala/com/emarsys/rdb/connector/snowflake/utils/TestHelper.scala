package com.emarsys.rdb.connector.snowflake.utils

import com.emarsys.rdb.connector.common.Models.PoolConfig
import com.emarsys.rdb.connector.snowflake.SnowflakeConnector.{SnowflakeConnectionConfig, SnowflakeConnectorConfig}
import com.typesafe.config.ConfigFactory

object TestHelper {
  val config = ConfigFactory.load().getConfig("dbconf")

  lazy val TEST_CONNECTION_CONFIG = SnowflakeConnectionConfig(
    accountName = config.getString("account_name"),
    warehouseName = config.getString("warehouse"),
    dbName = config.getString("database"),
    schemaName = config.getString("schema"),
    dbUser = config.getString("user"),
    dbPassword = config.getString("password"),
    connectionParams = config.getString("connectionParams")
  )

  val TEST_CONNECTOR_CONFIG = SnowflakeConnectorConfig(
    streamChunkSize = 5000,
    configPath = "snowflakedb",
    poolConfig = PoolConfig(2, 100)
  )
}
