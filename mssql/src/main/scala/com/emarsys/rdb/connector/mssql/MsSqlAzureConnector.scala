package com.emarsys.rdb.connector.mssql

import java.util.UUID

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors.{ConnectionConfigError, ConnectionError}
import com.emarsys.rdb.connector.common.models.{CommonConnectionReadableData, ConnectionConfig}
import com.emarsys.rdb.connector.mssql.MsSqlAzureConnector.{
  AzureMsSqlPort,
  MsSqlAzureConnectionConfig,
  MsSqlAzureConnectorConfig
}
import com.emarsys.rdb.connector.mssql.MsSqlConnector.MsSqlConnectorConfig
import com.typesafe.config.ConfigValueFactory.fromAnyRef
import com.typesafe.config.{Config, ConfigFactory}
import slick.jdbc.SQLServerProfile.api._

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}

object MsSqlAzureConnector extends MsSqlAzureConnectorTrait {

  val AzureMsSqlPort = 1433

  case class MsSqlAzureConnectionConfig(
      host: String,
      dbName: String,
      dbUser: String,
      dbPassword: String,
      connectionParams: String
  ) extends ConnectionConfig {
    override def toCommonFormat: CommonConnectionReadableData = {
      CommonConnectionReadableData("mssql-azure", s"$host:$AzureMsSqlPort", dbName, dbUser)
    }
  }

  case class MsSqlAzureConnectorConfig(
      configPath: String
  ) {
    def toMsSqlConnectorConfig: MsSqlConnectorConfig =
      MsSqlConnectorConfig(configPath, trustServerCertificate = true)
  }
}

trait MsSqlAzureConnectorTrait extends MsSqlConnectorHelper {
  import cats.instances.future._
  import cats.syntax.functor._

  val defaultConfig = MsSqlAzureConnectorConfig(
    configPath = "mssqldb"
  )

  def create(
      config: MsSqlAzureConnectionConfig,
      connectorConfig: MsSqlAzureConnectorConfig = defaultConfig
  )(implicit e: ExecutionContext): ConnectorResponse[MsSqlConnector] = {
    if (isSslDisabledOrTamperedWith(config.connectionParams)) {
      Future.successful(Left(ConnectionConfigError("SSL Error")))
    } else if (!checkAzureUrl(config.host)) {
      Future.successful(Left(ConnectionConfigError("Wrong Azure SQL host!")))
    } else {
      val poolName = UUID.randomUUID.toString
      val dbConfig = createDbConfig(config, poolName, connectorConfig)
      val database = Database.forConfig("", dbConfig)
      createMsSqlConnector(connectorConfig, poolName, database)
    }
  }

  private[mssql] def checkAzureUrl(url: String): Boolean = {
    url.endsWith(".database.windows.net")
  }

  private def createMsSqlConnector(connectorConfig: MsSqlAzureConnectorConfig, poolName: String, db: Database)(
      implicit ec: ExecutionContext
  ): Future[Either[ConnectionError, MsSqlConnector]] = {
    checkConnection(db)
      .as(Right(new MsSqlConnector(db, connectorConfig.toMsSqlConnectorConfig, poolName)))
      .recover {
        case ex =>
          db.shutdown
          Left(ConnectionError(ex))
      }
  }

  private def createDbConfig(
      config: MsSqlAzureConnectionConfig,
      poolName: String,
      connectorConfig: MsSqlAzureConnectorConfig
  ): Config = {
    val jdbcUrl = createUrl(config.host, AzureMsSqlPort, config.dbName, config.connectionParams)
    ConfigFactory
      .load()
      .getConfig(connectorConfig.configPath)
      .withValue("poolName", fromAnyRef(poolName))
      .withValue("registerMbeans", fromAnyRef(true))
      .withValue("properties.url", fromAnyRef(jdbcUrl))
      .withValue("properties.user", fromAnyRef(config.dbUser))
      .withValue("properties.password", fromAnyRef(config.dbPassword))
      .withValue("properties.driver", fromAnyRef("slick.jdbc.SQLServerProfile"))
      .withValue("properties.properties.encrypt", fromAnyRef(true))
      .withValue("properties.properties.trustServerCertificate", fromAnyRef(true))
  }
}
