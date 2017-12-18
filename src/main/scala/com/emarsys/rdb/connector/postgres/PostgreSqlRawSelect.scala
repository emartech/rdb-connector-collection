package com.emarsys.rdb.connector.postgres

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors.ErrorWithMessage
import com.emarsys.rdb.connector.common.models.SimpleSelect.FieldName
import slick.jdbc.MySQLProfile.api._

import scala.annotation.tailrec

trait PostgreSqlRawSelect extends PostgreSqlStreamingQuery {
  self: PostgreSqlConnector =>

  import PostgreSqlWriters._
  import com.emarsys.rdb.connector.common.defaults.SqlWriter._

  override def rawSelect(rawSql: String, limit: Option[Int]): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    val query = removeEndingSemicolons(rawSql)
    val limitedQuery = limit.fold(query){ l =>
      s"SELECT * FROM ( $query ) AS query LIMIT $l"
    }
    streamingQuery(limitedQuery)
  }

  override def validateRawSelect(rawSql: String): ConnectorResponse[Unit] = {
    val modifiedSql = "EXPLAIN " + removeEndingSemicolons(rawSql)
    db.run(sql"#$modifiedSql".as[(String)])
      .map(_ => Right())
      .recover {
        case ex => Left(ErrorWithMessage(ex.toString))
      }
  }

  override def analyzeRawSelect(rawSql: String): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    val modifiedSql = "EXPLAIN " + removeEndingSemicolons(rawSql)
    streamingQuery(modifiedSql)
  }

  override def projectedRawSelect(rawSql: String, fields: Seq[String]): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    val fieldList = fields.map("t." + FieldName(_).toSql).mkString(", ")
    val projectedSql = s"SELECT $fieldList FROM ( ${removeEndingSemicolons(rawSql)} ) t"
    streamingQuery(projectedSql)
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
