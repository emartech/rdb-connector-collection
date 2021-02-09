package com.emarsys.rdb.connector.hana

import com.emarsys.rdb.connector.hana.utils.TestHelper
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpecLike

class HanaConnectorItSpec extends AsyncWordSpecLike with Matchers with EitherValues {

  "HanaConnector" when {

    val testConnectorConfig = TestHelper.TEST_CONNECTOR_CONFIG
    val testConnection      = TestHelper.TEST_CONNECTION_CONFIG

    "create connector" should {

      "connect successfully" in {
        withClue("We should have received back a connector") {
          HanaConnector.createHanaCloudConnector(testConnection, testConnectorConfig).map { connector =>
            connector.value.close()
            succeed
          }
        }
      }
    }
  }
}
