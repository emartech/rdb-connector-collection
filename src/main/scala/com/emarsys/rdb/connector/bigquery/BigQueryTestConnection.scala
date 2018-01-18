package com.emarsys.rdb.connector.bigquery

import akka.stream.scaladsl.Sink
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors.ErrorWithMessage

import scala.concurrent.Future

trait BigQueryTestConnection {
  self: BigQueryConnector =>

  override def testConnection(): ConnectorResponse[Unit] = {
    streamingQuery("SELECT 1").flatMap{
      case Right(x) => x.runWith(Sink.seq).map{_ => Right()}
      case Left(x) => Future.successful(Left(x))
    }.recover { case _ => Left(ErrorWithMessage("Cannot connect to the sql server")) }
  }
}
