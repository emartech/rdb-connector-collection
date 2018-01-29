package com.emarsys.rdb.connector.bigquery

import akka.NotUsed
import akka.stream.scaladsl.{Sink, Source}
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors.ErrorWithMessage

import scala.annotation.tailrec
import scala.concurrent.Future

trait BigQueryRawSelect {
  self: BigQueryConnector =>

  override def rawSelect(rawSql: String, limit: Option[Int]): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    val query = removeEndingSemicolons(rawSql)
    val limitedQuery = limit.fold(query) { l =>
      wrapInLimit(query, l)
    }
    streamingQuery(limitedQuery)
  }

  override def projectedRawSelect(rawSql: String, fields: Seq[String]): ConnectorResponse[Source[Seq[String], NotUsed]] =
    runProjectedSelectWith(rawSql, fields, streamingQuery)

  override def validateProjectedRawSelect(rawSql: String, fields: Seq[String]): ConnectorResponse[Unit] = {
    runProjectedSelectWith(rawSql, fields, streamingDryQuery)
      .runWith(Sink.seq)
      .map(_ => Right({}))
      .recover { case ex => Left(ErrorWithMessage(ex.getMessage)) }
  }

  override def validateRawSelect(rawSql: String): ConnectorResponse[Unit] = {
    val modifiedSql = removeEndingSemicolons(rawSql)
    streamingDryQuery(modifiedSql)
      .runWith(Sink.seq)
      .map(_ => Right({}))
      .recover { case ex => Left(ErrorWithMessage(ex.getMessage)) }
  }

  override def analyzeRawSelect(rawSql: String): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    val modifiedSql = removeEndingSemicolons(rawSql)
    Future(Right(streamingDryQuery(modifiedSql)))
  }

  private def runProjectedSelectWith[R](rawSql: String, fields: Seq[String], queryRunner: String => R) = {
    val fieldList = concatenateProjection(fields)
    val projectedSql = wrapInProject(rawSql, fieldList)
    queryRunner(projectedSql)
  }

  private def wrapInLimit(query: String, l: Int) =
    s"SELECT * FROM ( $query ) AS query LIMIT $l"


  private def wrapInProject(rawSql: String, fieldList: String): String =
    s"SELECT $fieldList FROM ( ${removeEndingSemicolons(rawSql)} ) t"

  private def concatenateProjection(fields: Seq[String]) = {
    fields.map(fieldName => s"t.$fieldName").mkString(", ")
  }

  @tailrec
  private def removeEndingSemicolons(query: String): String = {
    val qTrimmed = query.trim
    if (qTrimmed.last == ';') {
      removeEndingSemicolons(qTrimmed.dropRight(1))
    } else {
      qTrimmed
    }
  }
}
