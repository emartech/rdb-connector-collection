package com.emarsys.rdb.connector.snowflake

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.ConnectorResponse
import slick.jdbc.{GetResult, PositionedResult}
import com.emarsys.rdb.connector.snowflake.SnowflakeProfile.api._

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

trait SnowflakeStreamingQuery {
  self: SnowflakeConnector =>

  protected def streamingQuery(
      timeout: FiniteDuration
  )(query: String): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    val sql = sql"#$query"
      .as(resultConverter)
      .withStatementParameters(
        fetchSize = Int.MinValue,
        statementInit = _.setQueryTimeout(timeout.toSeconds.toInt)
      )
    val publisher = db.stream(sql)
    val dbSource = Source
      .fromPublisher(publisher)
      .statefulMapConcat { () =>
        var first = true
        (data: (Seq[String], Seq[String])) =>
          if (first) {
            first = false
            List(data._1, data._2)
          } else {
            List(data._2)
          }
      }
      .recoverWithRetries(1, streamErrorHandler)

    Future.successful(Right(dbSource))
  }

  private val resultConverter: GetResult[(Seq[String], Seq[String])] = GetResult[(Seq[String], Seq[String])] { r =>
    val rowData = getRowData(r)
    val headers = getHeaders(r)
    (headers, rowData)
  }

  private def getRowData(result: PositionedResult): Seq[String] = {
    (0 until result.numColumns).map { i =>
      result.nextString()
    }
  }

  private def getHeaders(r: PositionedResult): Seq[String] =
    (1 to r.numColumns).map(r.rs.getMetaData.getColumnLabel(_))

}
