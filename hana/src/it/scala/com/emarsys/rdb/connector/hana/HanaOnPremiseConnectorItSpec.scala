package com.emarsys.rdb.connector.hana

import com.emarsys.rdb.connector.hana.utils.TestHelper
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpecLike

class HanaOnPremiseConnectorItSpec extends AsyncWordSpecLike with Matchers with EitherValues {

  "HanaConnector" when {

    val testConnection      = TestHelper.TEST_ON_PREMISE_CONNECTION_CONFIG
    val testConnectorConfig = TestHelper.TEST_ON_PREMISE_CONNECTOR_CONFIG

    "create on-premise connector" should {
      "connect successfully" in {
        withClue("We should have received back a connector") {
          HanaConnector.createHanaOnPremiseConnector(testConnection, testConnectorConfig).map { connector =>
            connector.value.close()
            succeed
          }
        }
      }

      "connect successfully without the schema specified explicitly" in {
        withClue("We should have received back a connector") {
          HanaConnector.createHanaOnPremiseConnector(testConnection.copy(schema = None), testConnectorConfig).map { connector =>
            connector.value.close()
            succeed
          }
        }
      }
    }
  }
}
