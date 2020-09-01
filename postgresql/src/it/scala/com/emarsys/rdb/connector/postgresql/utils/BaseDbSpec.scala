package com.emarsys.rdb.connector.postgresql.utils

import com.emarsys.rdb.connector.common.models.Connector
import com.emarsys.rdb.connector.postgresql.PostgreSqlConnector
import org.scalatest.EitherValues

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

trait BaseDbSpec extends EitherValues {
  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

  val timeout: FiniteDuration = 5.seconds

  lazy val connector: Connector =
    Await
      .result(
        PostgreSqlConnector.create(TestHelper.TEST_CONNECTION_CONFIG, TestHelper.TEST_CONNECTOR_CONFIG),
        5.seconds
      )
      .value

}
