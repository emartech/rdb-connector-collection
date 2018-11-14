package com.emarsys.rdb.connector.postgresql

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors.TableNotFound
import slick.jdbc.PostgresProfile.api._
import slick.jdbc.SQLActionBuilder

trait PostgreSqlIsOptimized {
  self: PostgreSqlConnector with PostgreSqlMetadata =>

  override def isOptimized(table: String, fields: Seq[String]): ConnectorResponse[Boolean] = {
    val query: SQLActionBuilder = sql"""select
              |    t.relname as table_name,
              |    i.relname as index_name,
              |    array_to_string(array_agg(a.attname), ', ') as column_names
              |from
              |    pg_class t,
              |    pg_class i,
              |    pg_index ix,
              |    pg_attribute a
              |where
              |    t.oid = ix.indrelid
              |    and i.oid = ix.indexrelid
              |    and a.attrelid = t.oid
              |    and a.attnum = ANY(ix.indkey)
              |    and t.relkind = 'r'
              |    and t.relname = $table
              |group by
              |    t.relname,
              |    i.relname
              |order by
              |    t.relname,
              |    i.relname;""".stripMargin

    db.run(query.as[(String, String, String)])
      .map(_.toList match {
        case Nil => Left(TableNotFound(table))
        case resultList =>
          Right(
            resultList.map(result => isOptimizedHelper(fields.map(_.toLowerCase), result._3.split(", "))).reduce(_ || _)
          )
      })
      .recover(eitherErrorHandler())
  }

  private def isOptimizedHelper(fields: Seq[String], resultFields: Seq[String]): Boolean = {
    fields match {
      case head :: Nil => head.equals(resultFields.head)
      case _           => resultFields.toSet.equals(fields.toSet)
    }
  }
}
