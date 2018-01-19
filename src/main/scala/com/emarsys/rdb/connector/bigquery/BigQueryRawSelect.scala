package com.emarsys.rdb.connector.bigquery

import akka.NotUsed
import akka.stream.scaladsl.{Sink, Source}
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.SimpleSelect.FieldName
import com.emarsys.rdb.connector.common.defaults.SqlWriter._
import com.emarsys.rdb.connector.common.models.Errors.ErrorWithMessage

import scala.annotation.tailrec
import scala.concurrent.Future

trait BigQueryRawSelect {
  self: BigQueryConnector =>

  override def rawSelect(rawSql: String, limit: Option[Int]): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    val query = removeEndingSemicolons(rawSql)
    val limitedQuery = limit.fold(query){ l =>
      s"SELECT * FROM ( $query ) AS query LIMIT $l"
    }
    streamingQuery(limitedQuery)
  }

  override def projectedRawSelect(rawSql: String, fields: Seq[String]): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    val fieldList = fields.map("t." + FieldName(_).toSql).mkString(", ")
    val projectedSql = s"SELECT $fieldList FROM ( ${removeEndingSemicolons(rawSql)} ) t"
    streamingQuery(projectedSql)
  }

  override def validateRawSelect(rawSql: String): ConnectorResponse[Unit] = {
    val modifiedSql = removeEndingSemicolons(rawSql)
    streamingDryQuery(modifiedSql).runWith(Sink.seq).map(_ => Right({})).recover{case x => Left(ErrorWithMessage(x.getMessage))}
  }

  override def analyzeRawSelect(rawSql: String): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    val modifiedSql = removeEndingSemicolons(rawSql)
    Future(Right(streamingDryQuery(modifiedSql)))
  }

  @tailrec
  private def removeEndingSemicolons(query: String): String = {
    val qTrimmed = query.trim
    if(qTrimmed.last == ';') {
      removeEndingSemicolons(qTrimmed.dropRight(1))
    } else {
      qTrimmed
    }
  }
}
