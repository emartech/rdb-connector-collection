package com.emarsys.rdb.connector.hana

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.hana.HanaProfile.api._

trait HanaTestConnection { self: HanaConnector =>
  override def testConnection(): ConnectorResponse[Unit] =
    run(sql"SELECT 1 FROM SYS.DUMMY".as[Int].map(_ => ()))
}
