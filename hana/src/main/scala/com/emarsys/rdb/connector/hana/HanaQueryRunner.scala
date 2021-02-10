package com.emarsys.rdb.connector.hana

import com.emarsys.rdb.connector.common.ConnectorResponse
import slick.dbio.{DBIOAction, NoStream}

trait HanaQueryRunner { self: HanaConnector =>
  def run[A](dbio: DBIOAction[A, NoStream, Nothing]): ConnectorResponse[A] =
    db.run(dbio).map(Right(_)).recover(eitherErrorHandler())
}
