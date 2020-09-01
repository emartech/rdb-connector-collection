package com.emarsys.rdb.connector.snowflake

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.snowflake.SnowflakeProfile.api._

trait SnowflakeTestConnection {
  self: SnowflakeConnector =>

  override def testConnection(): ConnectorResponse[Unit] = {
    db.run(sql"SELECT 1".as[Int])
      .map(_ => Right(()))
      .recover(eitherErrorHandler())
  }
}
