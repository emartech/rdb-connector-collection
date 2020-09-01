package com.emarsys.rdb.connector.postgresql.utils

import java.util.Properties

import com.emarsys.rdb.connector.common.Models.PoolConfig
import com.emarsys.rdb.connector.postgresql.PostgreSqlConnector.{
  createUrl,
  PostgreSqlConnectionConfig,
  PostgreSqlConnectorConfig
}
import slick.jdbc.PostgresProfile.api._
import slick.util.AsyncExecutor

import scala.concurrent.Future

object TestHelper {

  import com.typesafe.config.ConfigFactory

  lazy val config = ConfigFactory.load()

  lazy val TEST_CONNECTION_CONFIG = PostgreSqlConnectionConfig(
    host = config.getString("dbconf.host"),
    port = config.getInt("dbconf.port"),
    dbName = config.getString("dbconf.dbName"),
    dbUser = config.getString("dbconf.user"),
    dbPassword = config.getString("dbconf.password"),
    certificate = config.getString("dbconf.certificate"),
    connectionParams = config.getString("dbconf.connectionParams")
  )

  lazy val TEST_CONNECTOR_CONFIG = PostgreSqlConnectorConfig(
    streamChunkSize = 5000,
    configPath = "postgredb",
    sslMode = "verify-ca",
    poolConfig = PoolConfig(2, 100)
  )

  private lazy val db: Database = {
    Database.forURL(
      url = createUrl(TEST_CONNECTION_CONFIG),
      driver = "org.postgresql.Driver",
      user = TEST_CONNECTION_CONFIG.dbUser,
      password = TEST_CONNECTION_CONFIG.dbPassword,
      prop = new Properties(),
      executor = AsyncExecutor.default()
    )

  }

  def executeQuery(sql: String): Future[Int] = {
    db.run(sqlu"""#$sql""")
  }
}
