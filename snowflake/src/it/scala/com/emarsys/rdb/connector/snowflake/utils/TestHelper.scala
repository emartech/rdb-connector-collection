package com.emarsys.rdb.connector.snowflake.utils

import com.emarsys.rdb.connector.common.Models.PoolConfig
import com.emarsys.rdb.connector.snowflake.SnowflakeConnector.{
  createUrl,
  SnowflakeConnectionConfig,
  SnowflakeConnectorConfig
}
import com.typesafe.config.ConfigFactory
import com.emarsys.rdb.connector.snowflake.SnowflakeProfile.api._

import scala.concurrent.Future

object TestHelper {
  val config = ConfigFactory.load().getConfig("dbconf")

  lazy val TEST_CONNECTION_CONFIG = SnowflakeConnectionConfig(
    accountName = config.getString("account_name"),
    warehouseName = config.getString("warehouse"),
    dbName = config.getString("database"),
    schemaName = config.getString("schema"),
    dbUser = config.getString("user"),
    dbPassword = config.getString("password"),
    connectionParams = s"db=${config.getString("database")}"
  )

  val TEST_CONNECTOR_CONFIG = SnowflakeConnectorConfig(
    streamChunkSize = 5000,
    configPath = "snowflakedb",
    poolConfig = PoolConfig(2, 100)
  )

  private lazy val db: Database = {
    Database.forURL(
      createUrl(TEST_CONNECTION_CONFIG),
      Map(
        "driver"    -> "net.snowflake.client.jdbc.SnowflakeDriver",
        "user"      -> TEST_CONNECTION_CONFIG.dbUser,
        "password"  -> TEST_CONNECTION_CONFIG.dbPassword,
        "warehouse" -> TEST_CONNECTION_CONFIG.warehouseName,
        "schema"    -> TEST_CONNECTION_CONFIG.schemaName,
        "db"        -> TEST_CONNECTION_CONFIG.dbName
      )
    )
  }

  def executeQuery(sql: String): Future[Int] = {
    db.run(sqlu"""#$sql""")
  }
}
