package com.emarsys.rdb.connector.redshift

import com.emarsys.rdb.connector.common.ConnectorResponse
import slick.jdbc.PostgresProfile.api._

trait RedshiftTestConnection {
  self: RedshiftConnector =>

  override def testConnection(): ConnectorResponse[Unit] = {
    db.run(sql"SELECT 1".as[Int])
      .map(_ => Right(()))
      .recover(eitherErrorHandler)
  }
}
