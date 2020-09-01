package com.emarsys.rdb.connector.mssql.utils

import com.emarsys.rdb.connector.common.models.Connector
import com.emarsys.rdb.connector.mssql.MsSqlConnector
import com.emarsys.rdb.connector.test.util.EitherValues

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

trait BaseDbSpec extends EitherValues {

  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

  val timeout: FiniteDuration = 30.seconds

  val connector: Connector =
    Await
      .result(MsSqlConnector.create(TestHelper.TEST_CONNECTION_CONFIG, TestHelper.TEST_CONNECTOR_CONFIG), timeout)
      .value

}
