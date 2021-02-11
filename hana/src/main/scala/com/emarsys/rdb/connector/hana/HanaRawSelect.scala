package com.emarsys.rdb.connector.hana

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.SimpleSelect.FieldName

import scala.annotation.tailrec
import scala.concurrent.duration.FiniteDuration
import HanaProfile.api._
import slick.jdbc.{GetResult, PositionedResult}

import java.sql.ResultSet
import java.util.UUID

trait HanaRawSelect { self: HanaConnector =>
  import com.emarsys.rdb.connector.common.defaults.SqlWriter._
  import HanaWriters._

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

    runStreamingQuery(timeout)(limitedQuery)
  }

  override def validateRawSelect(rawSql: String): ConnectorResponse[Unit] = {
    val modifiedSql = wrapInExplain(removeEndingSemicolons(rawSql))
    run(sql"#$modifiedSql".as[Vector[AnyRef]](ignoredResult)).map(_.map(_ => ()))
  }

  override def analyzeRawSelect(rawSql: String): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    val query = createExplainPlanQuery(rawSql)

    run(query).map(_.map(result => Source(result.toList)))
  }

  private def createExplainPlanQuery(rawSql: String) = {
    SimpleDBIO[Seq[Seq[String]]] { context =>
      val id         = UUID.randomUUID().toString
      val connection = context.connection

      connection.createStatement().execute(s"""EXPLAIN PLAN SET STATEMENT_NAME = '$id' FOR $rawSql""")
      try {
        val explainPlanResultSet =
          connection.createStatement().executeQuery(s"SELECT OPERATOR_NAME, OPERATOR_DETAILS, OPERATOR_PROPERTIES, EXECUTION_ENGINE, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, TABLE_TYPE, TABLE_SIZE, OUTPUT_SIZE, SUBTREE_COST, OPERATOR_ID, PARENT_OPERATOR_ID, LEVEL, POSITION FROM explain_plan_table WHERE statement_name = '$id'")
        getResultWithColumnNames(explainPlanResultSet)
      } finally {
        connection.createStatement().execute(s"DELETE FROM explain_plan_table WHERE statement_name = '$id'")
      }
    }
  }

  private def getResultWithColumnNames(rs: ResultSet): Seq[Seq[String]] = {
    val columnIndexes = 1 to rs.getMetaData.getColumnCount

    var rows = Seq(columnIndexes.map(rs.getMetaData.getColumnLabel))
    while (rs.next()) rows :+= columnIndexes.map(rs.getString)

    rows
  }

  override def projectedRawSelect(
      rawSql: String,
      fields: Seq[String],
      limit: Option[Int],
      timeout: FiniteDuration,
      allowNullFieldValue: Boolean
  ): ConnectorResponse[Source[Seq[String], NotUsed]] =
    runProjectedSelectWith(rawSql, fields, limit, allowNullFieldValue, runStreamingQuery(timeout))

  override def validateProjectedRawSelect(rawSql: String, fields: Seq[String]): ConnectorResponse[Unit] = {
    runProjectedSelectWith(rawSql, fields, None, allowNullFieldValue = true, wrapInExplainThenRunOnDb)
      .map(_.map(_ => ()))
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

  private def wrapInExplainThenRunOnDb(query: String) =
    run(sql"#${wrapInExplain(query)}".as[Vector[AnyRef]](ignoredResult))

  private def wrapInExplain(sqlWithoutSemicolon: String) = {
    s"EXPLAIN PLAN FOR $sqlWithoutSemicolon"
  }

  private def wrapInExplainWithId(sqlWithoutSemicolon: String, id: String) = {
    s"""EXPLAIN PLAN SET STATEMENT_NAME = '$id' FOR $sqlWithoutSemicolon"""
  }

  private def concatenateProjection(fields: Seq[String]) =
    fields.map("t." + FieldName(_).toSql).mkString(", ")

  private def wrapInLimit(query: String, limit: Int): String = {
    s"SELECT * FROM ( $query ) LIMIT $limit"
  }

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
