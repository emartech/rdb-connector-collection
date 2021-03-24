package com.emarsys.rdb.connector.hana

import java.sql.Types

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.hana.HanaProfile.api._
import slick.dbio.{DBIOAction, NoStream}
import slick.jdbc.{GetResult, PositionedResult}

import scala.concurrent.Future

trait HanaQueryRunner { self: HanaConnector =>
  protected def run[A](dbio: DBIOAction[A, NoStream, Nothing]): ConnectorResponse[A] =
    db.run(dbio).map(Right(_)).recover(eitherErrorHandler())

  protected def runStreamingQuery(query: String): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    val sql = sql"#$query"
      .as(resultConverter)
      .transactionally
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
    (1 to result.numColumns).map { i =>
      result.rs.getMetaData.getColumnType(i) match {
        case Types.BOOLEAN => parseBoolean(result.nextString())
        case _             => result.nextString()
      }
    }
  }

  private def getHeaders(r: PositionedResult): Seq[String] =
    (1 to r.numColumns).map(r.rs.getMetaData.getColumnName(_))

  private def parseBoolean(column: String): String = column match {
    case "1" => "true"
    case "0" => "false"
    case _   => null
  }
}
