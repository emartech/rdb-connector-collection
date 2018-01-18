package com.emarsys.rdb.connector.bigquery

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.util.Timeout
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors.TableNotFound
import com.emarsys.rdb.connector.common.models._
import com.emarsys.rdb.connector.bigquery.BigQueryConnector.BigQueryConnectionConfig

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

class BigQueryConnector(protected val actorSystem: ActorSystem, val config: BigQueryConnectionConfig)
                       (implicit val executionContext: ExecutionContext)
  extends Connector
    with BigQueryWriter
    with BigQuerySimpleSelect
    with BigQueryRawSelect
    with BigQueryIsOptimized
    with BigQueryTestConnection
    with BigQueryMetadata {

  implicit val sys: ActorSystem = actorSystem
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val timeout: Timeout = Timeout(3.seconds)

  val googleTokenActor = actorSystem.actorOf(GoogleTokenActor.props(config.clientEmail, config.privateKey, Http()))

  protected def handleNotExistingTable[T](table: String): PartialFunction[Throwable, ConnectorResponse[T]] = {
    case e: Exception if e.getMessage.contains("doesn't exist") =>
      Future.successful(Left(TableNotFound(table)))
  }

  override def close(): Future[Unit] = Future.successful()

  override def validateRawSelect(rawSql: String): ConnectorResponse[Unit] = ???

  override def analyzeRawSelect(rawSql: String): ConnectorResponse[Source[Seq[String], NotUsed]] = ???
}

object BigQueryConnector extends BigQueryConnectorTrait {

  case class BigQueryConnectionConfig(
                                       projectId: String,
                                       dataset: String,
                                       clientEmail: String,
                                       privateKey: String,
                                     ) extends ConnectionConfig

}

trait BigQueryConnectorTrait extends ConnectorCompanion {

  def apply(config: BigQueryConnectionConfig)(actorSystem: ActorSystem): ConnectorResponse[BigQueryConnector] = {
    Future.successful(Right(new BigQueryConnector(actorSystem, config)(actorSystem.dispatcher)))
  }

  override def meta() = MetaData("`", "'", "\\")
}
