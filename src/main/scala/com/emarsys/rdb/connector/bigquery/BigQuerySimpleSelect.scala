package com.emarsys.rdb.connector.bigquery

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.defaults.SqlWriter._
import com.emarsys.rdb.connector.common.models.SimpleSelect

trait BigQuerySimpleSelect extends BigQueryStreamingQuery {
  self: BigQueryConnector =>

  override def simpleSelect(select: SimpleSelect): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    //get field types {
    val writer = BigQueryWriter(config, Seq())
    import writer._
    streamingQuery(select.toSql)
    // }
  }

}
