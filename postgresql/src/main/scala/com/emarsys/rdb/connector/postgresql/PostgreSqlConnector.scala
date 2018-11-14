package com.emarsys.rdb.connector.postgresql

import java.io.{File, PrintWriter}
import java.util.UUID

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors.{ConnectionConfigError, ConnectorError}
import com.emarsys.rdb.connector.common.models._
import com.emarsys.rdb.connector.postgresql.PostgreSqlConnector.{PostgreSqlConnectionConfig, PostgreSqlConnectorConfig}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import slick.jdbc.PostgresProfile.api._
import slick.util.AsyncExecutor

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class PostgreSqlConnector(
    protected val db: Database,
    protected val connectorConfig: PostgreSqlConnectorConfig,
    protected val poolName: String,
    protected val schemaName: String
)(implicit val executionContext: ExecutionContext)
    extends Connector
    with PostgreSqlErrorHandling
    with PostgreSqlTestConnection
    with PostgreSqlMetadata
    with PostgreSqlSimpleSelect
    with PostgreSqlRawSelect
    with PostgreSqlIsOptimized
    with PostgreSqlRawDataManipulation {

  override def close(): Future[Unit] = {
    db.shutdown
  }

  override def innerMetrics(): String = {
    import java.lang.management.ManagementFactory

    import com.zaxxer.hikari.HikariPoolMXBean
    import javax.management.{JMX, ObjectName}
    Try {
      val mBeanServer = ManagementFactory.getPlatformMBeanServer
      val poolObjectName =
        new ObjectName(s"com.zaxxer.hikari:type=Pool ($poolName)")
      val poolProxy = JMX.newMXBeanProxy(mBeanServer, poolObjectName, classOf[HikariPoolMXBean])

      s"""{
         |"activeConnections": ${poolProxy.getActiveConnections},
         |"idleConnections": ${poolProxy.getIdleConnections},
         |"threadAwaitingConnections": ${poolProxy.getThreadsAwaitingConnection},
         |"totalConnections": ${poolProxy.getTotalConnections}
         |}""".stripMargin
    }.getOrElse(super.innerMetrics)
  }
}

object PostgreSqlConnector extends PostgreSqlConnectorTrait {

  case class PostgreSqlConnectionConfig(
      host: String,
      port: Int,
      dbName: String,
      dbUser: String,
      dbPassword: String,
      certificate: String,
      connectionParams: String
  ) extends ConnectionConfig {
    override def toCommonFormat: CommonConnectionReadableData = {
      CommonConnectionReadableData("postgres", s"$host:$port", dbName, dbUser)
    }
  }

  case class PostgreSqlConnectorConfig(queryTimeout: FiniteDuration, streamChunkSize: Int)

}

trait PostgreSqlConnectorTrait extends ConnectorCompanion with PostgreSqlErrorHandling {
  private[postgresql] val defaultConfig = PostgreSqlConnectorConfig(
    queryTimeout = 20.minutes,
    streamChunkSize = 5000
  )

  override def meta() = MetaData("\"", "'", "\\")

  def apply(
      config: PostgreSqlConnectionConfig,
      connectorConfig: PostgreSqlConnectorConfig = defaultConfig,
      configPath: String = "postgredb"
  )(executor: AsyncExecutor)(implicit executionContext: ExecutionContext): ConnectorResponse[PostgreSqlConnector] = {
    val poolName = UUID.randomUUID.toString

    if (!checkSsl(config.connectionParams)) {
      Future.successful(Left(ConnectionConfigError("SSL Error")))
    } else {
      configureDb(config, configPath, poolName).flatMap(checkConnection(connectorConfig, poolName, config))
    }
  }

  private def configureDb(
      config: PostgreSqlConnector.PostgreSqlConnectionConfig,
      configPath: String,
      poolName: String
  ) = {
    val customDbConf = ConfigFactory
      .load()
      .getConfig(configPath)
      .withValue("poolName", ConfigValueFactory.fromAnyRef(poolName))
      .withValue("registerMbeans", ConfigValueFactory.fromAnyRef(true))
      .withValue("properties.url", ConfigValueFactory.fromAnyRef(createUrl(config)))
      .withValue("properties.user", ConfigValueFactory.fromAnyRef(config.dbUser))
      .withValue("properties.password", ConfigValueFactory.fromAnyRef(config.dbPassword))
      .withValue("properties.driver", ConfigValueFactory.fromAnyRef("org.postgresql.Driver"))
      .withValue("properties.properties.ssl", ConfigValueFactory.fromAnyRef("true"))
      .withValue("properties.properties.sslmode", ConfigValueFactory.fromAnyRef("verify-ca"))
      .withValue("properties.properties.loggerLevel", ConfigValueFactory.fromAnyRef("OFF"))
      .withValue(
        "properties.properties.sslrootcert",
        ConfigValueFactory.fromAnyRef(createTempFile(config.certificate))
      )

    Future.successful(Database.forConfig("", customDbConf))
  }

  private def checkConnection(
      connectorConfig: PostgreSqlConnectorConfig,
      poolName: String,
      config: PostgreSqlConnectionConfig
  )(db: Database)(implicit ec: ExecutionContext) =
    isConnectionAvailable(db) map [Either[ConnectorError, PostgreSqlConnector]] { _ =>
      Right(new PostgreSqlConnector(db, connectorConfig, poolName, createSchemaName(config)))
    } recover eitherErrorHandler() map {
      case Left(e) =>
        db.shutdown
        Left(e)
      case r => r
    }

  private def createTempFile(certificate: String): String = {
    val temp: File = File.createTempFile("root", ".crt")
    new PrintWriter(temp) {
      write(certificate)
      close
    }
    temp.deleteOnExit()
    temp.getAbsolutePath
  }

  private[postgresql] def checkSsl(connectionParams: String): Boolean = {
    !connectionParams.matches(".*ssl=false.*") &&
    !connectionParams.matches(".*sslmode=.*") &&
    !connectionParams.matches(".*sslrootcert=.*")
  }

  private def isConnectionAvailable(db: Database)(implicit executionContext: ExecutionContext): Future[Unit] = {
    db.run(sql"SELECT 1".as[(String)]).map(_ => {})
  }

  private[postgresql] def createUrl(config: PostgreSqlConnectionConfig) = {
    s"jdbc:postgresql://${config.host}:${config.port}/${config.dbName}${safeConnectionParams(config.connectionParams)}"
  }

  private def createSchemaName(config: PostgreSqlConnectionConfig) = {
    config.connectionParams
      .split("&")
      .toList
      .find(_.startsWith("currentSchema="))
      .flatMap(_.split("=").toList.tail.headOption)
      .getOrElse("public")
  }

  private def safeConnectionParams(connectionParams: String) = {
    if (connectionParams.startsWith("?") || connectionParams.isEmpty) {
      connectionParams
    } else {
      s"?$connectionParams"
    }
  }
}
