package com.emarsys.rdb.connector.hana

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.defaults.SqlWriter._
import com.emarsys.rdb.connector.common.models.SimpleSelect
import com.emarsys.rdb.connector.hana.HanaWriters._

import scala.concurrent.duration.FiniteDuration

trait HanaSimpleSelect {
  self: HanaConnector =>

  override def simpleSelect(
      select: SimpleSelect,
      timeout: FiniteDuration
  ): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    runStreamingQuery(timeout)(select.toSql)
  }

}
