package com.emarsys.rdb.connector.snowflake

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.SimpleSelect
import com.emarsys.rdb.connector.common.defaults.SqlWriter._
import SnowflakeSqlWriters._

import scala.concurrent.duration.FiniteDuration

trait SnowflakeSimpleSelect {
  self: SnowflakeConnector =>

  override def simpleSelect(
      select: SimpleSelect,
      timeout: FiniteDuration
  ): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    streamingQuery(timeout)(select.toSql)
  }

  // TODO: implement simpleSelectWithGroupLimit using window function
}
