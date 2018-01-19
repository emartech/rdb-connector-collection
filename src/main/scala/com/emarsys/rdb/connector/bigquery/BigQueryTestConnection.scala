package com.emarsys.rdb.connector.bigquery

import akka.stream.scaladsl.Sink
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors.ErrorWithMessage

import scala.concurrent.Future

trait BigQueryTestConnection {
  self: BigQueryConnector =>

  override def testConnection(): ConnectorResponse[Unit] = {
    streamingQuery("SELECT 1").flatMap{
      case Right(source) => source.runWith(Sink.seq).map{_ => Right()}
      case Left(error) => Future.successful(Left(error))
    }.recover { case _ => Left(ErrorWithMessage("Cannot connect to the sql server")) }
  }
}
