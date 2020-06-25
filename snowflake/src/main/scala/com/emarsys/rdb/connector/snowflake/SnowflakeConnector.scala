package com.emarsys.rdb.connector.snowflake

import java.util.UUID

import cats.data.EitherT
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.defaults.ErrorConverter
import com.emarsys.rdb.connector.common.Models.{CommonConnectionReadableData, ConnectionConfig, MetaData, PoolConfig}
import com.emarsys.rdb.connector.common.models.{Connector, ConnectorCompanion}
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName}
import com.emarsys.rdb.connector.snowflake.SnowflakeConnector.SnowflakeConnectorConfig
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import com.typesafe.config.ConfigValueFactory.fromAnyRef
import slick.jdbc.JdbcBackend.Database

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class SnowflakeConnector(
    protected val db: Database,
    protected val connectorConfig: SnowflakeConnectorConfig,
    protected val poolName: String,
    protected val schemaName: String
)(implicit val executionContext: ExecutionContext)
    extends Connector {
  override def close(): Future[Unit] = {
    db.shutdown
  }

  override def innerMetrics(): String = {
    import java.lang.management.ManagementFactory

    import com.zaxxer.hikari.HikariPoolMXBean
    import javax.management.{JMX, ObjectName}
    Try {
      val mBeanServer    = ManagementFactory.getPlatformMBeanServer
      val poolObjectName = new ObjectName(s"com.zaxxer.hikari:type=Pool ($poolName)")
      val poolProxy      = JMX.newMXBeanProxy(mBeanServer, poolObjectName, classOf[HikariPoolMXBean])

      s"""{
         |"activeConnections": ${poolProxy.getActiveConnections},
         |"idleConnections": ${poolProxy.getIdleConnections},
         |"threadAwaitingConnections": ${poolProxy.getThreadsAwaitingConnection},
         |"totalConnections": ${poolProxy.getTotalConnections}
         |}""".stripMargin
    }.getOrElse(super.innerMetrics())
  }
}

object SnowflakeConnector extends ConnectorCompanion {
  import cats.instances.future._
  import cats.syntax.functor._

  case class SnowflakeConnectionConfig(
      accountName: String,
      warehouseName: String,
      dbName: String,
      schemaName: String,
      dbUser: String,
      dbPassword: String,
      connectionParams: String
  ) extends ConnectionConfig {

    protected def getPublicFieldsForId = List(accountName, warehouseName, dbName, schemaName, dbUser, connectionParams)
    protected def getSecretFieldsForId = List(dbPassword)

    override def toCommonFormat: CommonConnectionReadableData = {
      CommonConnectionReadableData("snowflake", s"$accountName:$warehouseName", s"$dbName:$schemaName", dbUser)
    }
  }

  case class SnowflakeConnectorConfig(
      streamChunkSize: Int,
      configPath: String,
      poolConfig: PoolConfig
  )

  override def meta(): MetaData = MetaData("\"", "'", "\\")

  def create(
      connectionConfig: SnowflakeConnectionConfig,
      connectorConfig: SnowflakeConnectorConfig
  )(implicit ec: ExecutionContext): ConnectorResponse[SnowflakeConnector] =
    if (isSslDisabled(connectionConfig.connectionParams)) {
      Future.successful(Left(DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SSLError, "SSL is disabled")))
    } else {
      val poolName = UUID.randomUUID.toString
      val dbConfig = createDbConfig(connectionConfig, connectorConfig, poolName)
      val db       = Database.forConfig("", dbConfig)

      createConnector(connectorConfig, poolName, db, connectionConfig.schemaName)
    }

  private[snowflake] def isSslDisabled(connectionParams: String): Boolean = {
    connectionParams.matches(".*ssl=off.*")
  }

  private def createDbConfig(
      config: SnowflakeConnectionConfig,
      connectorConfig: SnowflakeConnectorConfig,
      poolName: String
  ): Config = {
    ConfigFactory
      .load()
      .getConfig(connectorConfig.configPath)
      .withValue("maxConnections", fromAnyRef(connectorConfig.poolConfig.maxPoolSize))
      .withValue("minConnections", fromAnyRef(connectorConfig.poolConfig.maxPoolSize))
      .withValue("numThreads", fromAnyRef(connectorConfig.poolConfig.maxPoolSize))
      .withValue("queueSize", fromAnyRef(connectorConfig.poolConfig.queueSize))
      .withValue("poolName", ConfigValueFactory.fromAnyRef(poolName))
      .withValue("registerMbeans", ConfigValueFactory.fromAnyRef(true))
      .withValue("url", ConfigValueFactory.fromAnyRef(createUrl(config)))
      .withValue("properties.user", ConfigValueFactory.fromAnyRef(config.dbUser))
      .withValue("properties.password", ConfigValueFactory.fromAnyRef(config.dbPassword))
      .withValue("properties.db", ConfigValueFactory.fromAnyRef(config.dbName))
      .withValue("properties.schema", ConfigValueFactory.fromAnyRef(config.schemaName))
      .withValue("properties.warehouse", ConfigValueFactory.fromAnyRef(config.warehouseName))
  }

  private[snowflake] def createUrl(config: SnowflakeConnectionConfig): String = {
    s"jdbc:snowflake://${config.accountName}.snowflakecomputing.com/${safeConnectionParams(config.connectionParams)}"
  }

  private def safeConnectionParams(connectionParams: String): String = {
    if (connectionParams.startsWith("?") || connectionParams.isEmpty) {
      connectionParams
    } else {
      s"?$connectionParams"
    }
  }

  private def createConnector(
      connectorConfig: SnowflakeConnectorConfig,
      poolName: String,
      db: Database,
      currentSchema: String
  )(implicit ec: ExecutionContext): ConnectorResponse[SnowflakeConnector] =
    EitherT(
      checkConnection(db)
        .as(Right(new SnowflakeConnector(db, connectorConfig, poolName, currentSchema)))
        .recover(ErrorConverter.default.andThen(Left.apply(_)))
    ).leftMap { connectionError =>
      db.close()
      connectionError
    }.value

  private def checkConnection(db: Database)(implicit executionContext: ExecutionContext): Future[Unit] = {
    import slick.jdbc.PostgresProfile.api._
    db.run(sql"SELECT 1".as[Int]).void
  }
}
