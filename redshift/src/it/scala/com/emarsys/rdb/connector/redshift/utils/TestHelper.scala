package com.emarsys.rdb.connector.redshift.utils

import java.util.Properties

import com.emarsys.rdb.connector.common.Models._
import com.emarsys.rdb.connector.redshift.RedshiftConnector.{createUrl, RedshiftConnectionConfig}
import slick.jdbc.PostgresProfile.api._
import slick.util.AsyncExecutor

import scala.concurrent.Future

object TestHelper {

  import com.typesafe.config.ConfigFactory

  val value = ConfigFactory.load().getString("dbconf.host")

  lazy val TEST_CONNECTION_CONFIG = RedshiftConnectionConfig(
    host = ConfigFactory.load().getString("dbconf.host"),
    port = ConfigFactory.load().getInt("dbconf.port"),
    dbName = ConfigFactory.load().getString("dbconf.dbName"),
    dbUser = ConfigFactory.load().getString("dbconf.user"),
    dbPassword = ConfigFactory.load().getString("dbconf.password"),
    connectionParams = ConfigFactory.load().getString("dbconf.connectionParams"),
    poolConfigs = PoolConfigs.empty
  )

  private lazy val db: Database = {
    Database.forURL(
      url = createUrl(TEST_CONNECTION_CONFIG),
      driver = "com.amazon.redshift.jdbc42.Driver",
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
