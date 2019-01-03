package com.emarsys.rdb.connector.mysql

import java.sql.SQLTransientException
import java.util.UUID

import cats.data.EitherT
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors._
import com.emarsys.rdb.connector.common.models._
import com.emarsys.rdb.connector.mysql.CertificateUtil.createKeystoreTempUrlFromCertificateString
import com.emarsys.rdb.connector.mysql.MySqlConnector.{MySqlConnectionConfig, MySqlConnectorConfig}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory, Config => TypesafeConfig}
import slick.jdbc.MySQLProfile.api._
import slick.jdbc.MySQLProfile.backend
import slick.util.AsyncExecutor

import scala.concurrent.duration._
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

  case class MySqlConnectorConfig(queryTimeout: FiniteDuration, streamChunkSize: Int)

}

trait MySqlConnectorTrait extends ConnectorCompanion with MySqlErrorHandling {
  import cats.instances.future._
  import cats.syntax.flatMap._

  val defaultConfig   = MySqlConnectorConfig(queryTimeout = 20.minutes, streamChunkSize = 5000)
  val useSSL: Boolean = Config.db.useSsl
  override def meta() = MetaData("`", "'", "\\")

  def apply(
      config: MySqlConnectionConfig,
      connectorConfig: MySqlConnectorConfig = defaultConfig,
      configPath: String = "mysqldb"
  )(executor: AsyncExecutor)(implicit e: ExecutionContext): ConnectorResponse[MySqlConnector] = {
    val poolName          = UUID.randomUUID.toString
    val jdbcUrl           = createJdbcUrl(config)
    val unsecuredDbConfig = createUnsecuredDbConfig(jdbcUrl, config.dbUser, config.dbPassword, configPath, poolName)

    if (useSSL) {
      createSecuredMySqlConnector(config.certificate, connectorConfig, poolName, useSslWithCertificateToDbConfig(unsecuredDbConfig))
    } else {
      createUnsecuredMySqlConnector(connectorConfig, poolName, unsecuredDbConfig)
    }
  }

  private def createSecuredMySqlConnector(
      certificate: String,
      connectorConfig: MySqlConnectorConfig,
      poolName: String,
      addSslToDbConfig: String => TypesafeConfig
  )(implicit e: ExecutionContext): Future[Either[ConnectorError, MySqlConnector]] = {
    (for {
      keystoreUrl <- createKeystoreUrl(certificate)
      database = Database.forConfig("", addSslToDbConfig(keystoreUrl))
      mySqlConnector <- createSecuredConnector(connectorConfig, poolName, database)
    } yield mySqlConnector).value
  }

  private def createKeystoreUrl(
      certificate: String
  )(implicit e: ExecutionContext): EitherT[Future, ConnectorError, String] = {
    EitherT
      .fromOption[Future](
        createKeystoreTempUrlFromCertificateString(certificate),
        ConnectionConfigError("Wrong SSL cert format"): ConnectorError
      )
  }

  private def useSslWithCertificateToDbConfig(unsecuredDbConfig: TypesafeConfig)(keystoreUrl: String): TypesafeConfig = {
    unsecuredDbConfig
      .withValue("properties.properties.useSSL", ConfigValueFactory.fromAnyRef("true"))
      .withValue("properties.properties.verifyServerCertificate", ConfigValueFactory.fromAnyRef("false"))
      .withValue("properties.properties.clientCertificateKeyStoreUrl", ConfigValueFactory.fromAnyRef(keystoreUrl))
  }

  private def createSecuredConnector(connectorConfig: MySqlConnectorConfig, poolName: String, db: backend.Database)(
      implicit ec: ExecutionContext
  ): EitherT[Future, ConnectorError, MySqlConnector] = {
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

  private def createUnsecuredMySqlConnector(
      connectorConfig: MySqlConnectorConfig,
      poolName: String,
      typesafeConfig: TypesafeConfig
  )(implicit e: ExecutionContext): Future[Either[ConnectorError, MySqlConnector]] = {
    val database = Database.forConfig("", typesafeConfig)
    Future.successful[Either[ConnectorError, MySqlConnector]](
      Right(new MySqlConnector(database, connectorConfig, poolName))
    )
  }

  private def createUnsecuredDbConfig(
      jdbcUrl: String,
      user: String,
      password: String,
      configPath: String,
      poolName: String
  ): TypesafeConfig = {
    dbConfig(jdbcUrl, user, password, configPath, poolName)
      .withValue("properties.properties.useSSL", ConfigValueFactory.fromAnyRef("false"))
      .withValue("properties.properties.verifyServerCertificate", ConfigValueFactory.fromAnyRef("false"))
  }

  private def dbConfig(
      jdbcUrl: String,
      user: String,
      password: String,
      configPath: String,
      poolName: String
  ): TypesafeConfig = {
    ConfigFactory
      .load()
      .getConfig(configPath)
      .withValue("poolName", ConfigValueFactory.fromAnyRef(poolName))
      .withValue("registerMbeans", ConfigValueFactory.fromAnyRef(true))
      .withValue("properties.url", ConfigValueFactory.fromAnyRef(jdbcUrl))
      .withValue("properties.user", ConfigValueFactory.fromAnyRef(user))
      .withValue("properties.password", ConfigValueFactory.fromAnyRef(password))
      .withValue("properties.driver", ConfigValueFactory.fromAnyRef("slick.jdbc.MySQLProfile"))
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
