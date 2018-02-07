package com.emarsys.rdb.connector.bigquery

import akka.actor.{ActorSystem, PoisonPill}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.emarsys.rdb.connector.bigquery.BigQueryConnector.BigQueryConnectionConfig
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.{CommonConnectionReadableData, _}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class BigQueryConnector(protected val actorSystem: ActorSystem, val config: BigQueryConnectionConfig)
                       (implicit val executionContext: ExecutionContext)
  extends Connector
    with BigQuerySimpleSelect
    with BigQueryRawSelect
    with BigQueryIsOptimized
    with BigQueryTestConnection
    with BigQueryMetadata {

  implicit val sys: ActorSystem = actorSystem
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val timeout: Timeout = Timeout(3.seconds)

  val googleTokenActor = actorSystem.actorOf(GoogleTokenActor.props(config.clientEmail, config.privateKey, Http()))

  override def close(): Future[Unit] = {
    googleTokenActor ! PoisonPill
    Future.unit
  }
}

object BigQueryConnector extends BigQueryConnectorTrait {

  case class BigQueryConnectionConfig(
                                       projectId: String,
                                       dataset: String,
                                       clientEmail: String,
                                       privateKey: String,
                                     ) extends ConnectionConfig {
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
