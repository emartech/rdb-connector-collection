package com.emarsys.rbd.connector.bigquery

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.emarsys.rbd.connector.bigquery.utils.TestHelper
import com.emarsys.rdb.connector.bigquery.BigQueryConnector
import com.emarsys.rdb.connector.common.models.Errors.ErrorWithMessage
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import concurrent.duration._
import scala.concurrent.Await

class BigQueryConnectorItSpec extends TestKit(ActorSystem()) with WordSpecLike with Matchers with BeforeAndAfterAll {

  override def afterAll {
    shutdown()
  }

  "BigQueryConnector" when {

    "#testConnection" should {

      "return ok in happy case" in {
        val connection = Await.result(BigQueryConnector(TestHelper.TEST_CONNECTION_CONFIG)(system), 3.seconds).toOption.get
        val result = Await.result(connection.testConnection(), 3.seconds)
        result shouldBe Right()
        connection.close()
      }

      "return error if cant connect" in {
        val badConnection = TestHelper.TEST_CONNECTION_CONFIG.copy(projectId = "asd")
        val connection = Await.result(BigQueryConnector(badConnection)(system), 3.seconds).toOption.get
        val result = Await.result(connection.testConnection(), 3.seconds)
        result shouldBe Left(ErrorWithMessage("Cannot connect to the sql server"))
      }

    }
  }
}
