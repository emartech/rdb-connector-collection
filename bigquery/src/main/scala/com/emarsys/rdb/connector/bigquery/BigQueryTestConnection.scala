package com.emarsys.rdb.connector.bigquery

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, HttpRequest}
import akka.stream.scaladsl.Sink
import cats.syntax.option._
import com.emarsys.rdb.connector.bigquery.stream.BigQueryStreamSource
import com.emarsys.rdb.connector.common.ConnectorResponse

trait BigQueryTestConnection {
  self: BigQueryConnector =>

  override def testConnection(): ConnectorResponse[Unit] = {
    val url            = GoogleApi.testConnectionUrl(config.projectId, config.dataset)
    val request        = HttpRequest(HttpMethods.GET, url)
    val bigQuerySource = BigQueryStreamSource(request, x => x.some, googleSession, Http())

    bigQuerySource
      .runWith(Sink.seq)
      .map(_ => Right({}))
      .recover(eitherErrorHandler)
  }
}
