package com.emarsys.rdb.connector.mysql

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors.TableNotFound
import com.emarsys.rdb.connector.common.models._
import com.emarsys.rdb.connector.mysql.BigQueryConnector.BigQueryConnectionConfig

import scala.concurrent.{ExecutionContext, Future}

class BigQueryConnector(protected val actorSystem: ActorSystem)(implicit val executionContext: ExecutionContext) extends Connector {

  protected def handleNotExistingTable[T](table: String): PartialFunction[Throwable, ConnectorResponse[T]] = {
    case e: Exception if e.getMessage.contains("doesn't exist") =>
      Future.successful(Left(TableNotFound(table)))
  }

  override def close(): Future[Unit] = ???

  override def testConnection(): ConnectorResponse[Unit] = ???

  override def listTables(): ConnectorResponse[Seq[TableSchemaDescriptors.TableModel]] = ???

  override def listTablesWithFields(): ConnectorResponse[Seq[TableSchemaDescriptors.FullTableModel]] = ???

  override def listFields(table: String): ConnectorResponse[Seq[TableSchemaDescriptors.FieldModel]] = ???

  override def isOptimized(table: String, fields: Seq[String]): ConnectorResponse[Boolean] = ???

  override def simpleSelect(select: SimpleSelect): ConnectorResponse[Source[Seq[String], NotUsed]] = ???

  override def rawSelect(rawSql: String, limit: Option[Int]): ConnectorResponse[Source[Seq[String], NotUsed]] = ???

  override def validateRawSelect(rawSql: String): ConnectorResponse[Unit] = ???

  override def analyzeRawSelect(rawSql: String): ConnectorResponse[Source[Seq[String], NotUsed]] = ???

  override def projectedRawSelect(rawSql: String, fields: Seq[String]): ConnectorResponse[Source[Seq[String], NotUsed]] = ???
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
    ???
  }

  override def meta() = MetaData("`", "'", "\\")
}
