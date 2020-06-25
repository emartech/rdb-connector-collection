package com.emarsys.rdb.connector.snowflake

import com.emarsys.rdb.connector.common.Models.{CommonConnectionReadableData, ConnectionConfig, MetaData, PoolConfig}
import com.emarsys.rdb.connector.common.models.{Connector, ConnectorCompanion}
import com.emarsys.rdb.connector.snowflake.SnowflakeConnector.SnowflakeConnectorConfig
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

  private[snowflake] def isSslDisabled(connectionParams: String): Boolean = {
    connectionParams.matches(".*ssl=off.*")
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
}
