package com.emarsys.rdb.connector.bigquery

import java.time.Clock

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.scaladsl.Source
import akka.util.Timeout
import com.emarsys.rdb.connector.bigquery.BigQueryConnector.BigQueryConnectionConfig
import com.emarsys.rdb.connector.common.Models.{CommonConnectionReadableData, ConnectionConfig, MetaData}
import com.emarsys.rdb.connector.common.models.DataManipulation.Criteria
import com.emarsys.rdb.connector.common.models.{Connector, ConnectorCompanion}
import com.emarsys.rdb.connector.common.{notImplementedOperation, ConnectorResponse}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class BigQueryConnector(protected val actorSystem: ActorSystem, val config: BigQueryConnectionConfig)(
    implicit val executionContext: ExecutionContext
) extends Connector
    with BigQueryErrorHandling
    with BigQuerySimpleSelect
    with BigQueryRawSelect
    with BigQueryIsOptimized
    with BigQueryTestConnection
    with BigQueryMetadata {

  implicit val sys: ActorSystem = actorSystem
  implicit val timeout: Timeout = Timeout(3.seconds)
  implicit val clock: Clock     = java.time.Clock.systemUTC()

  val googleSession                  = new GoogleSession(config.clientEmail, config.privateKey, new GoogleTokenApi(Http()))
  val bigQueryClient: BigQueryClient = new BigQueryClient(googleSession, config.projectId, config.dataset)

  override protected def rawSearch(
      tableName: String,
      criteria: Criteria,
      limit: Option[Int],
      timeout: FiniteDuration
  ): ConnectorResponse[Source[Seq[String], NotUsed]] = notImplementedOperation("rawSearch not implemented")

  override def close(): Future[Unit] = {
    Future.unit
  }
}

object BigQueryConnector extends BigQueryConnectorTrait {

  case class BigQueryConnectionConfig(projectId: String, dataset: String, clientEmail: String, privateKey: String)
      extends ConnectionConfig {

    protected def getPublicFieldsForId = List(projectId, dataset, clientEmail)
    protected def getSecretFieldsForId = List(privateKey)

    def toCommonFormat: CommonConnectionReadableData =
      CommonConnectionReadableData("bigquery", projectId, dataset, clientEmail)

  }

}

trait BigQueryConnectorTrait extends ConnectorCompanion {

  def apply(config: BigQueryConnectionConfig)(actorSystem: ActorSystem): ConnectorResponse[BigQueryConnector] = {
    Future.successful(Right(new BigQueryConnector(actorSystem, config)(actorSystem.dispatcher)))
  }

  override def meta() = MetaData("`", "'", "\\")
}
