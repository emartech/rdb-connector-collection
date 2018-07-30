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
    Future.successful(Right(bigQueryClient.streamingQuery(limitedQuery)))
  }

  override def projectedRawSelect(rawSql: String,
                                  fields: Seq[String],
                                  limit: Option[Int],
                                  allowNullFieldValue: Boolean): ConnectorResponse[Source[Seq[String], NotUsed]] =
    Future.successful(
      Right(runProjectedSelectWith(rawSql, fields, limit, allowNullFieldValue, query => bigQueryClient.streamingQuery(query)))
    )

  override def validateProjectedRawSelect(rawSql: String, fields: Seq[String]): ConnectorResponse[Unit] = {
    runProjectedSelectWith(rawSql,
                           fields,
                           None,
                           allowNullFieldValue = true,
                           query => bigQueryClient.streamingQuery(query, dryRun = true))
      .runWith(Sink.seq)
      .map(_ => Right({}))
      .recover(errorHandler())
  }

  override def validateRawSelect(rawSql: String): ConnectorResponse[Unit] = {
    val modifiedSql = removeEndingSemicolons(rawSql)
    bigQueryClient
      .streamingQuery(modifiedSql, dryRun = true)
      .runWith(Sink.seq)
      .map(_ => Right({}))
      .recover(errorHandler())
  }

  override def analyzeRawSelect(rawSql: String): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    val modifiedSql = removeEndingSemicolons(rawSql)
    Future(Right(bigQueryClient.streamingQuery(modifiedSql, dryRun = true)))
  }

  private def runProjectedSelectWith[R](rawSql: String,
                                        fields: Seq[String],
                                        limit: Option[Int],
                                        allowNullFieldValue: Boolean,
                                        queryRunner: String => R) = {
    val fieldList    = concatenateProjection(fields)
    val projectedSql = wrapInProjection(rawSql, fieldList)
    val query =
      if (!allowNullFieldValue) wrapInCondition(projectedSql, fields)
      else projectedSql
    val limitedQuery = limit.fold(query)(l => s"$query LIMIT $l")

    queryRunner(limitedQuery)
  }

  private def concatenateProjection(fields: Seq[String]) =
    fields.map("t." + _).mkString(", ")

  private def wrapInLimit(query: String, l: Int) =
    s"SELECT * FROM ( $query ) AS query LIMIT $l"

  private def wrapInCondition(rawSql: String, fields: Seq[String]): String =
    removeEndingSemicolons(rawSql) + concatenateCondition(fields)

  private def concatenateCondition(fields: Seq[String]) =
    " WHERE " + fields.map("t." + _ + " IS NOT NULL ").mkString("AND ")

  private def wrapInProjection(rawSql: String, projection: String) =
    s"SELECT $projection FROM ( ${removeEndingSemicolons(rawSql)} ) t"

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
