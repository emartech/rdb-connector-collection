package com.emarsys.rdb.connector.bigquery

import akka.NotUsed
import akka.stream.scaladsl.Source
import cats.data.EitherT
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.defaults.SqlWriter._
import com.emarsys.rdb.connector.common.models.SimpleSelect

import scala.concurrent.duration.FiniteDuration

trait BigQuerySimpleSelect {
  self: BigQueryConnector with BigQueryMetadata =>

  import cats.instances.future._

  override def simpleSelect(select: SimpleSelect, timeout: FiniteDuration): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    EitherT(listFields(select.table.t)).map { fields =>
      val writer = BigQueryWriter(config, fields)
      import writer._

      bigQueryClient.streamingQuery(select.toSql)
    }.value
  }
}
