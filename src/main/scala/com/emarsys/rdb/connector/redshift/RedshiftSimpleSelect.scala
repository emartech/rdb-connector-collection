package com.emarsys.rdb.connector.redshift

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.SimpleSelect

import com.emarsys.rdb.connector.common.defaults.SqlWriter._
import com.emarsys.rdb.connector.common.defaults.DefaultSqlWriters._

trait RedshiftSimpleSelect extends RedshiftStreamingQuery {
  self: RedshiftConnector =>

  override def simpleSelect(select: SimpleSelect): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    streamingQuery(select.toSql)
  }
}
