package com.emarsys.rdb.connector.hana.utils

import com.emarsys.rdb.connector.common.models.Connector
import com.emarsys.rdb.connector.hana.HanaConnector
import org.scalatest.EitherValues

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

trait BaseDbSpec extends EitherValues {
  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

  lazy val connector: Connector =
    Await
      .result(HanaConnector.createHanaCloudConnector(TestHelper.TEST_CONNECTION_CONFIG, TestHelper.TEST_CONNECTOR_CONFIG), 5.seconds)
      .value
}
