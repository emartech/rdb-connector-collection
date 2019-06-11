package com.emarsys.rdb.connector.mysql

import java.sql.SQLTransientException
import java.util.UUID

import cats.data.EitherT
import com.emarsys.rdb.connector.common.{ConnectorResponse, ConnectorResponseET}
import com.emarsys.rdb.connector.common.models.Errors._
import com.emarsys.rdb.connector.common.models._
import com.emarsys.rdb.connector.mysql.CertificateUtil.createTrustStoreTempUrl
import com.emarsys.rdb.connector.mysql.MySqlConnector.{MySqlConnectionConfig, MySqlConnectorConfig}
import com.typesafe.config.ConfigValueFactory.fromAnyRef
import com.typesafe.config.{Config, ConfigFactory}
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class MySqlConnector(
    protected val db: Database,
    protected val connectorConfig: MySqlConnectorConfig,
    protected val poolName: String
)(implicit val executionContext: ExecutionContext)
    extends Connector
    with MySqlErrorHandling
    with MySqlTestConnection
    with MySqlMetadata
    with MySqlSimpleSelect
    with MySqlRawSelect
    with MySqlIsOptimized
    with MySqlRawDataManipulation {

  override protected val fieldValueConverters = MysqlFieldValueConverters

  override val isErrorRetryable: PartialFunction[Throwable, Boolean] = {
    case _: SQLTransientException                                  => true
    case _: ConnectionTimeout                                      => true
    case ErrorWithMessage(message) if message.contains("Deadlock") => true
    case _: TransientDbError                                       => true
    case _                                                         => false
  }

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
    }.getOrElse(super.innerMetrics)
  }
}

object MySqlConnector extends MySqlConnectorTrait {

  final case class MySqlConnectionConfig(
      host: String,
      port: Int,
      dbName: String,
      dbUser: String,
      dbPassword: String,
      certificate: String,
      connectionParams: String,
      replicaConfig: Option[MySqlConnectionConfig] = None
  ) extends ConnectionConfig {
    override def replica[C <: MySqlConnectionConfig]: Option[C] = replicaConfig.map(_.asInstanceOf[C])

    override def toCommonFormat: CommonConnectionReadableData = {
      CommonConnectionReadableData("mysql", s"$host:$port", dbName, dbUser)
    }
  }

  case class MySqlConnectorConfig(
      configPath: String,
      verifyServerCertificate: Boolean
  )

}

trait MySqlConnectorTrait extends ConnectorCompanion with MySqlErrorHandling {
  import cats.instances.future._
  import cats.syntax.flatMap._

  val defaultConfig =
    MySqlConnectorConfig(
      configPath = "mysqldb",
      verifyServerCertificate = true
    )
  override def meta(): MetaData = MetaData("`", "'", "\\")

  def create(
      config: MySqlConnectionConfig,
      connectorConfig: MySqlConnectorConfig = defaultConfig
  )(implicit e: ExecutionContext): ConnectorResponse[MySqlConnector] = {
    (for {
      trustStoreUrl <- createTrustStoreUrl(config.certificate)
      poolName = UUID.randomUUID.toString
      dbConfig = createDbConfig(config, poolName, connectorConfig, trustStoreUrl)
      database = Database.forConfig("", dbConfig)
      mySqlConnector <- createMySqlConnector(connectorConfig, poolName, database)
    } yield mySqlConnector).value
  }

  private def createTrustStoreUrl(
      cert: String
  )(implicit e: ExecutionContext): ConnectorResponseET[String] = {
    EitherT
      .fromOption[Future](
        createTrustStoreTempUrl(cert),
        ConnectionConfigError("Wrong SSL cert format"): ConnectorError
      )
  }

  private def createMySqlConnector(connectorConfig: MySqlConnectorConfig, poolName: String, db: Database)(
      implicit ec: ExecutionContext
  ): ConnectorResponseET[MySqlConnector] = {
    EitherT(
      isSslUsedForConnection(db)
        .ifM(
          Future.successful(Right(new MySqlConnector(db, connectorConfig, poolName))),
          Future.successful(Left(ConnectionConfigError("SSL Error")))
        )
        .recover(eitherErrorHandler())
    ).leftMap { connectorError =>
      db.shutdown
      connectorError
    }
  }

  private def isSslUsedForConnection(db: Database)(implicit e: ExecutionContext): Future[Boolean] = {
    db.run(sql"SHOW STATUS LIKE 'ssl_cipher'".as[(String, String)])
      .map(ssl => ssl.head._2.contains("RSA-AES") || ssl.head._2.matches(".*AES\\d+-SHA.*"))
  }

  private def createDbConfig(
      config: MySqlConnectionConfig,
      poolName: String,
      connectorConfig: MySqlConnectorConfig,
      trustStoreUrl: String
  ): Config = {
    val jdbcUrl = createJdbcUrl(config)
    ConfigFactory
      .load()
      .getConfig(connectorConfig.configPath)
      .withValue("poolName", fromAnyRef(poolName))
      .withValue("registerMbeans", fromAnyRef(true))
      .withValue("properties.url", fromAnyRef(jdbcUrl))
      .withValue("properties.user", fromAnyRef(config.dbUser))
      .withValue("properties.password", fromAnyRef(config.dbPassword))
      .withValue("properties.driver", fromAnyRef("slick.jdbc.MySQLProfile"))
      .withValue("properties.properties.useSSL", fromAnyRef(true))
      .withValue("properties.properties.verifyServerCertificate", fromAnyRef(connectorConfig.verifyServerCertificate))
      .withValue("properties.properties.trustCertificateKeyStoreUrl", fromAnyRef(trustStoreUrl))
  }

  private[mysql] def createJdbcUrl(config: MySqlConnectionConfig): String = {
    s"jdbc:mysql://${config.host}:${config.port}/${config.dbName}${safeConnectionParams(config.connectionParams)}"
  }

  private def safeConnectionParams(connectionParams: String): String = {
    if (connectionParams.startsWith("?") || connectionParams.isEmpty) {
      connectionParams
    } else {
      s"?$connectionParams"
    }
  }
}
