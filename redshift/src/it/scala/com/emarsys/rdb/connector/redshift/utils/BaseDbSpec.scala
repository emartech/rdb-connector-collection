package com.emarsys.rdb.connector.redshift.utils

import com.emarsys.rdb.connector.common.models.Connector
import com.emarsys.rdb.connector.redshift.RedshiftConnector
import org.scalatest.EitherValues

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

trait BaseDbSpec extends EitherValues {
  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

  val connectionConfig = TestHelper.TEST_CONNECTION_CONFIG
  val connectorConfig  = TestHelper.TEST_CONNECTOR_CONFIG

  lazy val connector: Connector =
    Await
      .result(RedshiftConnector.create(connectionConfig, connectorConfig), 5.seconds)
      .right
      .value

}
