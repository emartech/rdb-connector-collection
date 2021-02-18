package com.emarsys.rdb.connector.hana.utils

import java.util.UUID

import com.emarsys.rdb.connector.common.Models.PoolConfig
import com.emarsys.rdb.connector.hana.HanaConnector
import com.emarsys.rdb.connector.hana.HanaConnector.{
  HanaCloudConnectionConfig,
  HanaConnectorConfig,
  HanaOnPremiseConnectionConfig
}
import com.emarsys.rdb.connector.hana.HanaProfile.api._
import slick.jdbc.JdbcBackend.Database

import scala.concurrent.Future

object TestHelper {
  import com.typesafe.config.ConfigFactory
  lazy val config = ConfigFactory.load()

  lazy val TEST_CLOUD_CONNECTION_CONFIG = HanaCloudConnectionConfig(
    instanceId = config.getString("cloud-dbconf.instance-id"),
    dbUser = config.getString("cloud-dbconf.user"),
    dbPassword = config.getString("cloud-dbconf.password"),
    schema = Some(config.getString("cloud-dbconf.schema")),
    connectionParams = config.getString("cloud-dbconf.connectionParams")
  )

  lazy val TEST_CLOUD_CONNECTOR_CONFIG = HanaConnectorConfig(
    configPath = "hana-db",
    validateCertificate = true,
    poolConfig = PoolConfig(2, 100)
  )

  lazy val TEST_ON_PREMISE_CONNECTION_CONFIG = HanaOnPremiseConnectionConfig(
    host = config.getString("on-premise-dbconf.host"),
    port = config.getInt("on-premise-dbconf.port"),
    dbName = config.getString("on-premise-dbconf.db"),
    dbUser = config.getString("on-premise-dbconf.user"),
    dbPassword = config.getString("on-premise-dbconf.password"),
    schema = Some(config.getString("on-premise-dbconf.schema")),
    certificate = config.getString("on-premise-dbconf.certificate"),
    connectionParams = config.getString("on-premise-dbconf.connectionParams")
  )

  lazy val TEST_ON_PREMISE_CONNECTOR_CONFIG = TEST_CLOUD_CONNECTOR_CONFIG

  private lazy val db: Database = {
    val poolName = UUID.randomUUID.toString
    val dbConfig =
      HanaConnector.createCloudDbConfig(TEST_CLOUD_CONNECTION_CONFIG, TEST_CLOUD_CONNECTOR_CONFIG, poolName)
    Database.forConfig("", dbConfig)
  }

  def executeQuery(sql: String): Future[Int] = {
    db.run(sqlu"""#$sql""")
  }
}
