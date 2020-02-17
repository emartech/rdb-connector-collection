package com.emarsys.rdb.connector.mssql.utils
import java.util.Properties

import com.emarsys.rdb.connector.common.Models._
import com.emarsys.rdb.connector.mssql.CertificateUtil
import com.emarsys.rdb.connector.mssql.MsSqlConnector.{MsSqlConnectionConfig, createUrl}
import slick.jdbc.MySQLProfile.api._
import slick.util.AsyncExecutor

import scala.concurrent.Future

object TestHelper {
  import com.typesafe.config.ConfigFactory
  lazy val config = ConfigFactory.load()

  lazy val TEST_CONNECTION_CONFIG = MsSqlConnectionConfig(
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
    val prop = new Properties()
    prop.setProperty("encrypt", "false")
    prop.setProperty("trustServerCertificate", "false")

    val url = createUrl(
      TEST_CONNECTION_CONFIG.host,
      TEST_CONNECTION_CONFIG.port,
      TEST_CONNECTION_CONFIG.dbName,
      TEST_CONNECTION_CONFIG.connectionParams
    )

    Database.forURL(
      url = url,
      driver = "slick.jdbc.SQLServerProfile",
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
