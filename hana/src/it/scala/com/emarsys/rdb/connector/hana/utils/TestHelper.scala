package com.emarsys.rdb.connector.hana.utils

import java.util.UUID

import com.emarsys.rdb.connector.common.Models.PoolConfig
import com.emarsys.rdb.connector.hana.HanaConnector
import com.emarsys.rdb.connector.hana.HanaConnector.{HanaCloudConnectionConfig, HanaConnectorConfig}
import com.emarsys.rdb.connector.hana.HanaProfile.api._
import slick.jdbc.JdbcBackend.Database

import scala.concurrent.Future

object TestHelper {
  import com.typesafe.config.ConfigFactory
  lazy val config = ConfigFactory.load()

  lazy val TEST_CONNECTION_CONFIG = HanaCloudConnectionConfig(
    instanceId = config.getString("cloud-dbconf.instance-id"),
    dbUser = config.getString("cloud-dbconf.user"),
    dbPassword = config.getString("cloud-dbconf.password"),
    schema = config.getString("cloud-dbconf.schema"),
    connectionParams = config.getString("cloud-dbconf.connectionParams")
  )

  lazy val TEST_CONNECTOR_CONFIG = HanaConnectorConfig(
    configPath = "hana-cloud-db",
    validateCertificate = true,
    poolConfig = PoolConfig(2, 100)
  )

  private lazy val db: Database = {
    val poolName = UUID.randomUUID.toString
    val dbConfig = HanaConnector.createDbConfig(TEST_CONNECTION_CONFIG, TEST_CONNECTOR_CONFIG, poolName)
    Database.forConfig("", dbConfig)
  }

  def executeQuery(sql: String): Future[Int] = {
    db.run(sqlu"""#$sql""")
  }
}
