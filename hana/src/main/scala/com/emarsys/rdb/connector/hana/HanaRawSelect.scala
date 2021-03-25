package com.emarsys.rdb.connector.hana

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.SimpleSelect.FieldName
import com.emarsys.rdb.connector.hana.HanaProfile.api._
import slick.jdbc.{GetResult, PositionedResult}

import java.sql.ResultSet
import java.util.UUID
import scala.annotation.tailrec
import scala.concurrent.duration.FiniteDuration

trait HanaRawSelect { self: HanaConnector =>
  import HanaWriters._
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

    runStreamingQuery(limitedQuery)
  }

  override def validateRawSelect(rawSql: String): ConnectorResponse[Unit] =
    testQueryWithExplain(removeEndingSemicolons(rawSql))

  override def analyzeRawSelect(rawSql: String): ConnectorResponse[Source[Seq[String], NotUsed]] =
    explainQuery(removeEndingSemicolons(rawSql)).map(_.map(result => Source(result.toList)))

  override def projectedRawSelect(
      rawSql: String,
      fields: Seq[String],
      limit: Option[Int],
      timeout: FiniteDuration,
      allowNullFieldValue: Boolean
  ): ConnectorResponse[Source[Seq[String], NotUsed]] =
    runProjectedSelectWith(rawSql, fields, limit, allowNullFieldValue, runStreamingQuery)

  override def validateProjectedRawSelect(rawSql: String, fields: Seq[String]): ConnectorResponse[Unit] = {
    runProjectedSelectWith(rawSql, fields, None, allowNullFieldValue = true, testQueryWithExplain)
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

  private def testQueryWithExplain(query: String) = {
    val id = UUID.randomUUID().toString

    val explainAction = for {
      _ <- sql"#${wrapInExplain(query, id)}".as(ignoredResult)
      _ <- sql"DELETE FROM explain_plan_table WHERE statement_name = $id".asUpdate
    } yield ()

    run(explainAction)
  }

  private def explainQuery(query: String) = {
    val id = UUID.randomUUID().toString

    val explainAction = for {
      _           <- sql"#${wrapInExplain(query, id)}".as(ignoredResult)
      explainPlan <- selectExplainResultAction(id)
      _           <- sql"DELETE FROM explain_plan_table WHERE statement_name = $id".asUpdate
    } yield explainPlan

    run(explainAction)
  }

  private def selectExplainResultAction(id: String) = {
    SimpleDBIO { context =>
      val connection = context.connection
      val selectExplainResultQuery =
        s"""SELECT
           |	OPERATOR_NAME,
           |	OPERATOR_DETAILS,
           |	OPERATOR_PROPERTIES,
           |	EXECUTION_ENGINE,
           |	DATABASE_NAME,
           |	SCHEMA_NAME,
           |	TABLE_NAME,
           |	TABLE_TYPE,
           |	TABLE_SIZE,
           |	OUTPUT_SIZE,
           |	SUBTREE_COST,
           |	OPERATOR_ID,
           |	PARENT_OPERATOR_ID,
           |	LEVEL,
           |	POSITION
           |FROM explain_plan_table
           |WHERE statement_name = '$id'""".stripMargin

      val explainPlanResultSet = connection.createStatement().executeQuery(selectExplainResultQuery)
      getResultWithColumnNames(explainPlanResultSet)
    }

  }

  private def getResultWithColumnNames(rs: ResultSet): Seq[Seq[String]] = {
    val columnIndexes = 1 to rs.getMetaData.getColumnCount

    val rowsBuilder = Seq.newBuilder[Seq[String]]

    rowsBuilder += columnIndexes.map(rs.getMetaData.getColumnLabel)
    while (rs.next()) rowsBuilder += columnIndexes.map(rs.getString)

    rowsBuilder.result()
  }

  private def wrapInExplain(sqlWithoutSemicolon: String, id: String) = {
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
