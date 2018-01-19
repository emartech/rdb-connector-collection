package com.emarsys.rdb.connector.bigquery

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors.TableNotFound

trait BigQueryIsOptimized {
  self: BigQueryConnector =>

  override def isOptimized(table: String, fields: Seq[String]): ConnectorResponse[Boolean] = {
    listTables()
      .map(_.map(_.exists(_.name == table))
        .flatMap(if (_) Right(true) else Left(TableNotFound(table))))
  }
}
