package com.emarsys.rdb.connector.hana.utils

import com.emarsys.rdb.connector.common.Models.PoolConfig
import com.emarsys.rdb.connector.hana.HanaConnector.{HanaCloudConnectionConfig, HanaConnectorConfig}

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
}
