package com.emarsys.rdb.connector.redshift

import java.util.UUID

import cats.data.EitherT
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.Models.{CommonConnectionReadableData, ConnectionConfig, MetaData, PoolConfig}
import com.emarsys.rdb.connector.common.models.{Connector, ConnectorCompanion}
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName}
import com.emarsys.rdb.connector.common.models.SimpleSelect.TableName
import com.emarsys.rdb.connector.redshift.RedshiftConnector.{RedshiftConnectionConfig, RedshiftConnectorConfig}
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import com.typesafe.config.ConfigValueFactory.fromAnyRef
import slick.jdbc.PostgresProfile.api._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class RedshiftConnector(
    protected val db: Database,
    protected val connectorConfig: RedshiftConnectorConfig,
    protected val poolName: String,
    protected val schemaName: String
)(implicit val executionContext: ExecutionContext)
    extends Connector
    with RedshiftTestConnection
    with RedshiftErrorHandling
    with RedshiftMetadata
    with RedshiftSimpleSelect
    with RedshiftRawSelect
    with RedshiftIsOptimized
    with RedshiftRawDataManipulation {

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

object RedshiftConnector extends RedshiftConnectorTrait {

  case class RedshiftConnectionConfig(
      host: String,
      port: Int,
      dbName: String,
      dbUser: String,
      dbPassword: String,
      connectionParams: String
  ) extends ConnectionConfig[RedshiftConnectionConfig] {

    protected def getPublicFieldsForId = List(host, port.toString, dbName, dbUser, connectionParams)
    protected def getSecretFieldsForId = List(dbPassword)

    override def toCommonFormat: CommonConnectionReadableData = {
      CommonConnectionReadableData("redshift", s"$host:$port", dbName, dbUser)
    }
  }

  case class RedshiftConnectorConfig(
                                      streamChunkSize: Int,
                                      configPath: String,
                                      poolConfig: PoolConfig
                                    )

}

trait RedshiftConnectorTrait extends ConnectorCompanion with RedshiftErrorHandling {
  import cats.instances.future._
  import cats.syntax.functor._
  import com.emarsys.rdb.connector.common.defaults.DefaultSqlWriters._
  import com.emarsys.rdb.connector.common.defaults.SqlWriter._

  override def meta(): MetaData = MetaData("\"", "'", "\\")

  def create(
              config: RedshiftConnectionConfig,
              connectorConfig: RedshiftConnectorConfig
  )(implicit ec: ExecutionContext): ConnectorResponse[RedshiftConnector] = {
    if (isSslDisabled(config.connectionParams)) {
      Future.successful(Left(DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SSLError, "SSL is disabled")))
    } else {
      val poolName      = UUID.randomUUID.toString
      val currentSchema = createSchemaName(config)
      val dbConfig      = createDbConfig(config, connectorConfig, poolName, currentSchema)
      val db            = Database.forConfig("", dbConfig)

      createMsSqlConnector(connectorConfig, poolName, db, currentSchema).value
    }
  }

  private def createDbConfig(
      config: RedshiftConnectionConfig,
      connectorConfig: RedshiftConnectorConfig,
      poolName: String,
      currentSchema: String
  ): Config = {
    val setSchemaQuery = s"set search_path to ${TableName(currentSchema).toSql}"
    ConfigFactory
      .load()
      .getConfig(connectorConfig.configPath)
      .withValue("maxConnections", fromAnyRef(connectorConfig.poolConfig.maxPoolSize))
      .withValue("minConnections", fromAnyRef(connectorConfig.poolConfig.maxPoolSize))
      .withValue("numThreads", fromAnyRef(connectorConfig.poolConfig.maxPoolSize))
      .withValue("queueSize", fromAnyRef(connectorConfig.poolConfig.queueSize))
      .withValue("poolName", ConfigValueFactory.fromAnyRef(poolName))
      .withValue("connectionInitSql", ConfigValueFactory.fromAnyRef(setSchemaQuery))
      .withValue("registerMbeans", ConfigValueFactory.fromAnyRef(true))
      .withValue("jdbcUrl", ConfigValueFactory.fromAnyRef(createUrl(config)))
      .withValue("username", ConfigValueFactory.fromAnyRef(config.dbUser))
      .withValue("password", ConfigValueFactory.fromAnyRef(config.dbPassword))
  }

  private def createMsSqlConnector(
      connectorConfig: RedshiftConnectorConfig,
      poolName: String,
      db: Database,
      currentSchema: String
  )(implicit ec: ExecutionContext): EitherT[Future, DatabaseError, RedshiftConnector] = {
    EitherT(
      checkConnection(db)
        .as(Right(new RedshiftConnector(db, connectorConfig, poolName, currentSchema)))
        .recover(eitherErrorHandler)
    ).leftMap { connectionError =>
      db.close()
      connectionError
    }
  }

  private[redshift] def isSslDisabled(connectionParams: String): Boolean = {
    connectionParams.matches(".*ssl=false.*")
  }

  protected def checkConnection(db: Database)(implicit executionContext: ExecutionContext): Future[Unit] = {
    db.run(sql"SELECT 1".as[Int]).void
  }

  private[redshift] def createUrl(config: RedshiftConnectionConfig): String = {
    s"jdbc:redshift://${config.host}:${config.port}/${config.dbName}${safeConnectionParams(config.connectionParams)}"
  }

  private def createSchemaName(config: RedshiftConnectionConfig): String = {
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
