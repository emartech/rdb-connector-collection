package com.emarsys.rdb.connector.snowflake

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.SimpleSelect
import com.emarsys.rdb.connector.snowflake.SnowflakeSqlWriters.selectWithGroupLimitWriter

import scala.concurrent.duration.FiniteDuration

trait SnowflakeSimpleSelect {
  self: SnowflakeConnector =>
  import com.emarsys.rdb.connector.common.defaults.DefaultSqlWriters._
  import com.emarsys.rdb.connector.common.defaults.SqlWriter._

  override def simpleSelect(
      select: SimpleSelect,
      timeout: FiniteDuration
  ): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    streamingQuery(timeout)(select.toSql)
  }

  override def runSelectWithGroupLimit(
      select: SimpleSelect,
      groupLimit: Int,
      references: Seq[String],
      timeout: FiniteDuration
  ): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    val sql = select.toSql(selectWithGroupLimitWriter(groupLimit, references))
    streamingQuery(timeout)(sql).map(_.map(_.map(_.dropRight(1))))
  }
}
