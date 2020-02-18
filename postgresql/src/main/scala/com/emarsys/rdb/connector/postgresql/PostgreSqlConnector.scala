package com.emarsys.rdb.connector.postgresql

import java.io.{File, PrintWriter}
import java.util.UUID

import cats.data.EitherT
import com.emarsys.rdb.connector.common.{ConnectorResponse, ConnectorResponseET}
import com.emarsys.rdb.connector.common.Models.{CommonConnectionReadableData, ConnectionConfig, MetaData, PoolConfig}
import com.emarsys.rdb.connector.common.models.{Connector, ConnectorCompanion}
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName}
import com.emarsys.rdb.connector.postgresql.PostgreSqlConnector.{PostgreSqlConnectionConfig, PostgreSqlConnectorConfig}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.config.ConfigValueFactory.fromAnyRef
import slick.jdbc.PostgresProfile.api._

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
  ) extends ConnectionConfig[PostgreSqlConnectionConfig] {

    protected def getPublicFieldsForId = List(host, port.toString, dbName, dbUser, connectionParams)
    protected def getSecretFieldsForId = List(dbPassword, certificate)

    override def toCommonFormat: CommonConnectionReadableData = {
      CommonConnectionReadableData("postgres", s"$host:$port", dbName, dbUser)
    }
  }

  case class PostgreSqlConnectorConfig(
      streamChunkSize: Int,
      configPath: String,
      sslMode: String,
      poolConfig: PoolConfig
  )

}

trait PostgreSqlConnectorTrait extends ConnectorCompanion with PostgreSqlErrorHandling {
  import cats.instances.future._
  import cats.syntax.functor._

  override def meta(): MetaData = MetaData("\"", "'", "\\")

  def create(
      config: PostgreSqlConnectionConfig,
      connectorConfig: PostgreSqlConnectorConfig
  )(implicit ec: ExecutionContext): ConnectorResponse[PostgreSqlConnector] = {
    val poolName = UUID.randomUUID.toString

    if (isSslDisabledOrTamperedWith(config.connectionParams)) {
      Future.successful(
        Left(DatabaseError(ErrorCategory.Internal, ErrorName.ConnectionConfigError, "SSL Error", None, None))
      )
    } else {
      val dbConfig = createDbConfig(config, connectorConfig, poolName)
      val database = Database.forConfig("", dbConfig)
      createPostgreSqlConnector(connectorConfig, poolName, config, database).value
    }
  }

  private def createDbConfig(
      config: PostgreSqlConnectionConfig,
      connectorConfig: PostgreSqlConnectorConfig,
      poolName: String
  ): Config = {
    val jdbcUrl      = createUrl(config)
    val rootCertPath = createTempFile(config.certificate)
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
      .withValue("driver", fromAnyRef("org.postgresql.Driver"))
      .withValue("properties.ssl", fromAnyRef("true"))
      .withValue("properties.sslmode", fromAnyRef(connectorConfig.sslMode))
      .withValue("properties.loggerLevel", fromAnyRef("OFF"))
      .withValue("properties.sslrootcert", fromAnyRef(rootCertPath))
  }

  private def createPostgreSqlConnector(
      connectorConfig: PostgreSqlConnectorConfig,
      poolName: String,
      config: PostgreSqlConnectionConfig,
      db: Database
  )(implicit ec: ExecutionContext): ConnectorResponseET[PostgreSqlConnector] = {
    EitherT(
      checkConnection(db)
        .as(Right(new PostgreSqlConnector(db, connectorConfig, poolName, createSchemaName(config))))
        .recover(eitherErrorHandler())
    ).leftMap { connectionErrors =>
      db.close()
      connectionErrors
    }
  }

  // TODO rewrite
  private def createTempFile(certificate: String): String = {
    val tempFile = File.createTempFile("root", ".crt")
    tempFile.deleteOnExit()
    val writer = new PrintWriter(tempFile)
    writer.write(certificate)
    writer.close()
    tempFile.getAbsolutePath
  }

  private[postgresql] def isSslDisabledOrTamperedWith(connectionParams: String): Boolean = {
    connectionParams.matches(".*ssl=false.*") ||
    connectionParams.matches(".*sslmode=.*") ||
    connectionParams.matches(".*sslrootcert=.*")
  }

  private def checkConnection(db: Database)(implicit ec: ExecutionContext): Future[Unit] = {
    db.run(sql"SELECT 1".as[String]).void
  }

  private[postgresql] def createUrl(config: PostgreSqlConnectionConfig): String = {
    s"jdbc:postgresql://${config.host}:${config.port}/${config.dbName}${safeConnectionParams(config.connectionParams)}"
  }

  private def createSchemaName(config: PostgreSqlConnectionConfig): String = {
    config.connectionParams
      .split("&")
      .toList
      .find(_.startsWith("currentSchema="))
      .flatMap(_.split("=").toList.tail.headOption)
      .getOrElse("public")
  }

  private def safeConnectionParams(connectionParams: String): String = {
    if (connectionParams.startsWith("?") || connectionParams.isEmpty) {
      connectionParams
    } else {
      s"?$connectionParams"
    }
  }
}
