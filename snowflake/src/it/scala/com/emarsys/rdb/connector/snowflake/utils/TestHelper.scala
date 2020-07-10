package com.emarsys.rdb.connector.snowflake.utils

import java.util.UUID

import com.emarsys.rdb.connector.common.Models.PoolConfig
import com.emarsys.rdb.connector.snowflake.SnowflakeConnector
import com.emarsys.rdb.connector.snowflake.SnowflakeConnector.{SnowflakeConnectionConfig, SnowflakeConnectorConfig}
import com.typesafe.config.ConfigFactory
import com.emarsys.rdb.connector.snowflake.SnowflakeProfile.api._
import slick.jdbc.JdbcBackend.Database

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
    connectionParams = s""
  )

  val TEST_CONNECTOR_CONFIG = SnowflakeConnectorConfig(
    streamChunkSize = 5000,
    configPath = "snowflakedb",
    poolConfig = PoolConfig(2, 100)
  )

  private lazy val db: Database = {
    val poolName = UUID.randomUUID.toString
    val dbConfig = SnowflakeConnector.createDbConfig(TEST_CONNECTION_CONFIG, TEST_CONNECTOR_CONFIG, poolName)
    Database.forConfig("", dbConfig)
  }

  def executeQuery(sql: String): Future[Int] = {
    db.run(sqlu"""#$sql""")
  }
}
