package com.emarsys.rdb.connector.bigquery

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.ConnectorResponse

trait BigQueryStreamingQuery {
  self: BigQueryConnector =>

  protected def streamingQuery(query: String): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    ???
  }
}
