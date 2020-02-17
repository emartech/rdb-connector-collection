package com.emarsys.rdb.connector.mysql.utils

import java.util.Properties

import com.emarsys.rdb.connector.common.Models._
import com.emarsys.rdb.connector.mysql.CertificateUtil
import com.emarsys.rdb.connector.mysql.MySqlConnector.{MySqlConnectionConfig, createJdbcUrl}
import slick.jdbc.MySQLProfile.api._
import slick.util.AsyncExecutor

import scala.concurrent.Future

object TestHelper {
  import com.typesafe.config.ConfigFactory
  lazy val config = ConfigFactory.load()

  lazy val TEST_CONNECTION_CONFIG = MySqlConnectionConfig(
    host = config.getString("dbconf.host"),
    port = config.getString("dbconf.port").toInt,
    dbName = config.getString("dbconf.dbName"),
    dbUser = config.getString("dbconf.user"),
    dbPassword = config.getString("dbconf.password"),
    certificate = config.getString("dbconf.certificate"),
    connectionParams = config.getString("dbconf.connectionParams"),
    connectorConfig = ConnectorConfig(
      select = PoolConfig(1, 1),
      update = PoolConfig(1, 1),
      segment = PoolConfig(1, 1),
      meta = PoolConfig(1, 1),
      test = PoolConfig(1, 1)
    )
  )

  private lazy val db: Database = {
    val keystoreUrl = CertificateUtil.createTrustStoreTempUrl(TEST_CONNECTION_CONFIG.certificate).get

    val prop = new Properties()
    prop.setProperty("useSSL", "true")
    prop.setProperty("verifyServerCertificate", "false")
    prop.setProperty("clientCertificateKeyStoreUrl", keystoreUrl)

    val url = createJdbcUrl(TEST_CONNECTION_CONFIG)

    Database.forURL(
      url = url,
      driver = "slick.jdbc.MySQLProfile",
      user = TEST_CONNECTION_CONFIG.dbUser,
      password = TEST_CONNECTION_CONFIG.dbPassword,
      prop = prop,
      executor = AsyncExecutor.default()
    )
  }

  def executeQuery(sql: String): Future[Int] = {
    db.run(sqlu"""#$sql""")
  }
}
