package com.emarsys.rdb.connector.mysql

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.defaults.SqlWriter._
import com.emarsys.rdb.connector.common.models.SimpleSelect.TableName
import com.emarsys.rdb.connector.mysql.MySqlWriters._
import slick.jdbc.MySQLProfile.api._

trait MySqlIsOptimized {
  self: MySqlConnector =>

  override def isOptimized(table: String, fields: Seq[String]): ConnectorResponse[Boolean] = {
    val fieldSet = fields.toSet
    db.run(
        sql"SHOW INDEX FROM #${TableName(table).toSql}"
          .as[(String, String, String, String, String, String, String, String, String, String, String, String, String)]
      )
      .map(_.groupBy(_._3).map { case (_, b) => b.map(_._5) })
      .map(_.exists(indexGroup => indexGroup.toSet == fieldSet || Set(indexGroup.head) == fieldSet))
      .map(Right(_))
      .recover(handleNotExistingTable(table))
      .recover(eitherErrorHandler())
  }
}
