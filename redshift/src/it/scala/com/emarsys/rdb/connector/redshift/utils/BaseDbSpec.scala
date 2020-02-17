package com.emarsys.rdb.connector.redshift.utils

import com.emarsys.rdb.connector.common.models.Connector
import com.emarsys.rdb.connector.redshift.RedshiftConnector
import org.scalatest.EitherValues

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

trait BaseDbSpec extends EitherValues {
  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

  lazy val connector: Connector =
    Await
      .result(RedshiftConnector.create(TestHelper.TEST_CONNECTION_CONFIG, TestHelper.TEST_CONNECTOR_CONFIG), 5.seconds)
      .right
      .value

}
