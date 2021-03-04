package com.emarsys.rdb.connector.hana

import cats.data.EitherT
import com.emarsys.rdb.connector.common.Models.{CommonConnectionReadableData, ConnectionConfig, MetaData, PoolConfig}
import com.emarsys.rdb.connector.common.defaults.ErrorConverter
import com.emarsys.rdb.connector.common.models.Errors.DatabaseError
import com.emarsys.rdb.connector.common.models.{Connector, ConnectorCompanion}
import com.emarsys.rdb.connector.common.{ConnectorResponse, ConnectorResponseET}
import com.emarsys.rdb.connector.hana.HanaConnector.{
  HanaCloudConnectionConfig,
  HanaConnectorConfig,
  HanaOnPremiseConnectionConfig
}
import com.emarsys.rdb.connector.hana.HanaProfile.api._
import com.typesafe.config.ConfigValueFactory.fromAnyRef
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

import java.util.UUID

class HanaConnector(
    protected val db: Database,
    protected val connectorConfig: HanaConnectorConfig,
    protected val poolName: String
)(implicit val executionContext: ExecutionContext)
    extends Connector
    with HanaQueryRunner
    with HanaErrorHandling
    with HanaTestConnection
    with HanaSimpleSelect
    with HanaRawSelect
    with HanaMetadata {

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
    }.getOrElse(super.innerMetrics())
  }
}

object HanaConnector extends HanaConnectorTrait {

  val UNDEFINED_SCHEMA = "schema is not defined"

  case class HanaCloudConnectionConfig(
      instanceId: String,
      dbUser: String,
      dbPassword: String,
      schema: Option[String],
      connectionParams: String
  ) extends ConnectionConfig {

    protected def getPublicFieldsForId = List(instanceId, schema.getOrElse(UNDEFINED_SCHEMA), dbUser, connectionParams)
    protected def getSecretFieldsForId = List(dbPassword)

    override def toCommonFormat: CommonConnectionReadableData = {
      CommonConnectionReadableData("hana-cloud", instanceId, schema.getOrElse(UNDEFINED_SCHEMA), dbUser)
    }
  }

  case class HanaOnPremiseConnectionConfig(
      host: String,
      port: Int,
      dbName: String,
      dbUser: String,
      dbPassword: String,
      certificate: String,
      schema: Option[String],
      connectionParams: String
  ) extends ConnectionConfig {

    protected def getPublicFieldsForId =
      List(host, port.toString, dbName, dbUser, schema.getOrElse(UNDEFINED_SCHEMA), connectionParams)
    protected def getSecretFieldsForId = List(dbPassword, certificate)

    override def toCommonFormat: CommonConnectionReadableData = {
      CommonConnectionReadableData(
        "hana-on-premise",
        s"$host:$port",
        s"$dbName/${schema.getOrElse(UNDEFINED_SCHEMA)}",
        dbUser
      )
    }
  }

  case class HanaConnectorConfig(
      configPath: String,
      validateCertificate: Boolean,
      poolConfig: PoolConfig
  )
}

trait HanaConnectorTrait extends ConnectorCompanion {
  import cats.instances.future._
  import cats.syntax.functor._

  override def meta(): MetaData = MetaData("\"", "'", "\\")

  def createHanaCloudConnector(
      config: HanaCloudConnectionConfig,
      connectorConfig: HanaConnectorConfig
  )(implicit ec: ExecutionContext): ConnectorResponse[HanaConnector] = {
    val poolName = UUID.randomUUID.toString
    val dbConfig = createCloudDbConfig(config, connectorConfig, poolName)
    val database = Database.forConfig("", dbConfig)

    createHanaConnector(connectorConfig, poolName, database).value
  }

  def createHanaOnPremiseConnector(
      config: HanaOnPremiseConnectionConfig,
      connectorConfig: HanaConnectorConfig
  )(implicit ec: ExecutionContext): ConnectorResponse[HanaConnector] = {
    val poolName = UUID.randomUUID.toString
    val dbConfig = createOnPremiseDbConfig(config, connectorConfig, poolName)
    val database = Database.forConfig("", dbConfig)

    createHanaConnector(connectorConfig, poolName, database).value
  }

  private[hana] def createCloudDbConfig(
      config: HanaCloudConnectionConfig,
      connectorConfig: HanaConnectorConfig,
      poolName: String
  ): Config = {
    val jdbcUrl = createCloudUrl(config)
    val conf = createDbConfig(connectorConfig, poolName)
      .withValue("jdbcUrl", fromAnyRef(jdbcUrl))
      .withValue("username", fromAnyRef(config.dbUser))
      .withValue("password", fromAnyRef(config.dbPassword))

    config.schema.fold(conf) { schema =>
      conf.withValue("properties.currentSchema", fromAnyRef(schema))
    }
  }

  private def createOnPremiseDbConfig(
      config: HanaOnPremiseConnectionConfig,
      connectorConfig: HanaConnectorConfig,
      poolName: String
  ): Config = {
    val jdbcUrl = createOnPremiseUrl(config)
    val conf = createDbConfig(connectorConfig, poolName)
      .withValue("jdbcUrl", fromAnyRef(jdbcUrl))
      .withValue("username", fromAnyRef(config.dbUser))
      .withValue("password", fromAnyRef(config.dbPassword))
      .withValue("properties.databaseName", fromAnyRef(config.dbName))
      .withValue(
        "properties.sslTrustStore",
        fromAnyRef(config.certificate.replaceAll("\n", ""))
      )

    config.schema.fold(conf) { schema =>
      conf.withValue("properties.currentSchema", fromAnyRef(schema))
    }
  }

  private def createDbConfig(connectorConfig: HanaConnectorConfig, poolName: String): Config = ConfigFactory
    .load()
    .getConfig(connectorConfig.configPath)
    .withValue("maxConnections", fromAnyRef(connectorConfig.poolConfig.maxPoolSize))
    .withValue("minConnections", fromAnyRef(connectorConfig.poolConfig.maxPoolSize))
    .withValue("numThreads", fromAnyRef(connectorConfig.poolConfig.maxPoolSize))
    .withValue("queueSize", fromAnyRef(connectorConfig.poolConfig.queueSize))
    .withValue("poolName", fromAnyRef(poolName))
    .withValue("registerMbeans", fromAnyRef(true))
    .withValue("driver", fromAnyRef("com.sap.db.jdbc.Driver"))
    .withValue("properties.encrypt", fromAnyRef(true))
    .withValue("properties.validateCertificate", fromAnyRef(connectorConfig.validateCertificate))

  private def createHanaConnector(
      connectorConfig: HanaConnectorConfig,
      poolName: String,
      db: Database
  )(implicit ec: ExecutionContext): ConnectorResponseET[HanaConnector] = {
    EitherT(
      checkConnection(db)
        .as[Either[DatabaseError, HanaConnector]](Right(new HanaConnector(db, connectorConfig, poolName)))
        .recover(ErrorConverter.default.andThen(Left.apply(_)))
    ).leftMap { connectionErrors =>
      db.close()
      connectionErrors
    }
  }

  private def checkConnection(db: Database)(implicit ec: ExecutionContext): Future[Unit] = {
    db.run(sql"SELECT 1 FROM SYS.DUMMY;".as[Int]).void
  }

  private def createCloudUrl(config: HanaCloudConnectionConfig): String = {
    s"jdbc:sap://${config.instanceId}.hanacloud.ondemand.com:443/${safeConnectionParams(config.connectionParams)}"
  }

  private def createOnPremiseUrl(config: HanaOnPremiseConnectionConfig): String = {
    s"jdbc:sap://${config.host}:${config.port}/${safeConnectionParams(config.connectionParams)}"
  }

  private def safeConnectionParams(connectionParams: String): String = {
    if (connectionParams.startsWith("?") || connectionParams.isEmpty) {
      connectionParams
    } else {
      s"?$connectionParams"
    }
  }
}
