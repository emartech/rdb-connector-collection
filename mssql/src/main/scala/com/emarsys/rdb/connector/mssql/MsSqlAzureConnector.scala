package com.emarsys.rdb.connector.mssql

import java.util.UUID

import cats.data.EitherT
import cats.syntax.applicativeError._
import com.emarsys.rdb.connector.common.{ConnectorResponse, ConnectorResponseET}
import com.emarsys.rdb.connector.common.Models.{CommonConnectionReadableData, ConnectionConfig, PoolConfig}
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName}
import com.emarsys.rdb.connector.mssql.MsSqlAzureConnector.{
  AzureMsSqlPort,
  MsSqlAzureConnectionConfig,
  MsSqlAzureConnectorConfig
}
import com.emarsys.rdb.connector.mssql.MsSqlConnector.MsSqlConnectorConfig
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.config.ConfigValueFactory.fromAnyRef
import slick.jdbc.SQLServerProfile.api._

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

    protected def getPublicFieldsForId = List(host, dbName, dbUser, connectionParams)
    protected def getSecretFieldsForId = List(dbPassword)

    override def toCommonFormat: CommonConnectionReadableData = {
      CommonConnectionReadableData("mssql-azure", s"$host:$AzureMsSqlPort", dbName, dbUser)
    }
  }

  case class MsSqlAzureConnectorConfig(
      configPath: String,
      poolConfig: PoolConfig
  ) {
    def toMsSqlConnectorConfig: MsSqlConnectorConfig =
      MsSqlConnectorConfig(configPath, trustServerCertificate = true, poolConfig)
  }
}

trait MsSqlAzureConnectorTrait extends MsSqlErrorHandling with MsSqlConnectorHelper {
  import cats.instances.future._
  import cats.syntax.functor._

//  val defaultConfig = MsSqlAzureConnectorConfig(
//    configPath = "mssqldb"
//  )

  def create(
      config: MsSqlAzureConnectionConfig,
      connectorConfig: MsSqlAzureConnectorConfig
  )(implicit e: ExecutionContext): ConnectorResponse[MsSqlConnector] = {
    if (isSslDisabledOrTamperedWith(config.connectionParams)) {
      Future.successful(
        Left(DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SSLError, "SSL is disabled", None, None))
      )
    } else if (!checkAzureUrl(config.host)) {
      Future.successful(
        Left(
          DatabaseError(
            ErrorCategory.FatalQueryExecution,
            ErrorName.ConnectionConfigError,
            s"Wrong Azure SQL host: ${config.host}",
            None,
            None
          )
        )
      )
    } else {
      val poolName = UUID.randomUUID.toString
      val dbConfig = createDbConfig(config, poolName, connectorConfig)
      val database = Database.forConfig("", dbConfig)
      createMsSqlConnector(connectorConfig, poolName, database).value
    }
  }

  private[mssql] def checkAzureUrl(url: String): Boolean = {
    url.endsWith(".database.windows.net")
  }

  private def createMsSqlConnector(connectorConfig: MsSqlAzureConnectorConfig, poolName: String, db: Database)(
      implicit ec: ExecutionContext
  ): ConnectorResponseET[MsSqlConnector] = {
    EitherT(
      checkConnection(db)
        .as(Right(new MsSqlConnector(db, connectorConfig.toMsSqlConnectorConfig, poolName)))
        .recover(eitherErrorHandler())
    ).onError {
      case _ =>
        EitherT.liftF(db.shutdown.recover {
          case _ =>
            // we don't have logging here to log the shutdownError
            ()
        })
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
      .withValue("maxConnections", fromAnyRef(connectorConfig.poolConfig.maxPoolSize))
      .withValue("minConnections", fromAnyRef(connectorConfig.poolConfig.maxPoolSize))
      .withValue("numThreads", fromAnyRef(connectorConfig.poolConfig.maxPoolSize))
      .withValue("queueSize", fromAnyRef(connectorConfig.poolConfig.queueSize))
      .withValue("poolName", fromAnyRef(poolName))
      .withValue("registerMbeans", fromAnyRef(true))
      .withValue("jdbcUrl", fromAnyRef(jdbcUrl))
      .withValue("username", fromAnyRef(config.dbUser))
      .withValue("password", fromAnyRef(config.dbPassword))
      .withValue("properties.encrypt", fromAnyRef(true))
      .withValue("properties.trustServerCertificate", fromAnyRef(true))
  }
}
