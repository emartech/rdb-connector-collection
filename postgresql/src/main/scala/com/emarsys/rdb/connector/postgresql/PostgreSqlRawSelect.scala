package com.emarsys.rdb.connector.postgresql

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.SimpleSelect.FieldName
import slick.jdbc.MySQLProfile.api._
import slick.jdbc.{GetResult, PositionedResult}

import scala.annotation.tailrec
import scala.concurrent.duration.{FiniteDuration, SECONDS}

trait PostgreSqlRawSelect extends PostgreSqlStreamingQuery {
  self: PostgreSqlConnector =>

  import PostgreSqlWriters._
  import com.emarsys.rdb.connector.common.defaults.SqlWriter._

  private val ignoredResult: GetResult[Vector[AnyRef]] = (_: PositionedResult) => Vector.empty

  override def rawSelect(
      rawSql: String,
      limit: Option[Int],
      timeout: FiniteDuration
  ): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    val query = removeEndingSemicolons(rawSql)
    val limitedQuery = limit.fold(query) { l =>
      wrapInLimit(query, l)
    }
    streamingQuery(timeout)(limitedQuery)
  }

  override def validateRawSelect(rawSql: String): ConnectorResponse[Unit] = {
    val modifiedSql = wrapInExplain(removeEndingSemicolons(rawSql))
    runQueryOnDb(modifiedSql)
      .map(_ => Right(()))
      .recover(eitherErrorHandler())
  }

  private def runQueryOnDb(sanitized: String) = {
    db.run(sql"#$sanitized".as[Vector[AnyRef]](ignoredResult))
  }

  private def wrapInExplain(sqlWithoutSemicolon: String) = {
    "EXPLAIN " + sqlWithoutSemicolon
  }

  override def analyzeRawSelect(rawSql: String): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    val modifiedSql = wrapInExplain(removeEndingSemicolons(rawSql))
    streamingQuery(FiniteDuration(5, SECONDS))(modifiedSql)
  }

  private def runProjectedSelectWith[R](
      rawSql: String,
      fields: Seq[String],
      limit: Option[Int],
      allowNullFieldValue: Boolean,
      queryRunner: String => R
  ) = {
    val fieldList    = concatenateProjection(fields)
    val projectedSql = wrapInProjection(rawSql, fieldList)
    val query =
      if (!allowNullFieldValue) wrapInCondition(projectedSql, fields)
      else projectedSql
    val limitedQuery = limit.fold(query)(l => s"$query LIMIT $l")

    queryRunner(limitedQuery)
  }

  override def projectedRawSelect(
      rawSql: String,
      fields: Seq[String],
      limit: Option[Int],
      timeout: FiniteDuration,
      allowNullFieldValue: Boolean
  ): ConnectorResponse[Source[Seq[String], NotUsed]] =
    runProjectedSelectWith(rawSql, fields, limit, allowNullFieldValue, streamingQuery(timeout))

  override def validateProjectedRawSelect(rawSql: String, fields: Seq[String]): ConnectorResponse[Unit] = {
    runProjectedSelectWith(rawSql, fields, None, allowNullFieldValue = true, wrapInExplainThenRunOnDb)
      .map(_ => Right(()))
      .recover(eitherErrorHandler())
  }
  private def wrapInExplainThenRunOnDb(query: String) =
    runQueryOnDb(wrapInExplain(query))

  private def concatenateProjection(fields: Seq[String]) =
    fields.map("t." + FieldName(_).toSql).mkString(", ")

  private def wrapInLimit(query: String, l: Int) =
    s"SELECT * FROM ( $query ) AS query LIMIT $l"

  private def wrapInCondition(rawSql: String, fields: Seq[String]): String =
    removeEndingSemicolons(rawSql) + concatenateCondition(fields)

  private def concatenateCondition(fields: Seq[String]) =
    " WHERE " + fields.map("t." + FieldName(_).toSql + " IS NOT NULL ").mkString("AND ")

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
