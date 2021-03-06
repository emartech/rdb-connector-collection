package com.emarsys.rdb.connector.hana

import com.emarsys.rdb.connector.common.models.Errors._
import com.emarsys.rdb.connector.hana.utils.TestHelper
import com.emarsys.rdb.connector.test.CustomMatchers.beDatabaseErrorEqualWithoutCause
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpecLike

class HanaCloudConnectorItSpec extends AsyncWordSpecLike with Matchers with EitherValues {

  val timeoutMessage = "Connection is not available, request timed out after"

  "HanaConnector" when {

    val testConnection      = TestHelper.TEST_CLOUD_CONNECTION_CONFIG
    val testConnectorConfig = TestHelper.TEST_CLOUD_CONNECTOR_CONFIG

    "create cloud connector" should {
      "connect successfully" in {
        withClue("We should have received back a connector") {
          HanaConnector.createHanaCloudConnector(testConnection, testConnectorConfig).map { connector =>
            connector.value.close()
            succeed
          }
        }
      }

      "connect successfully without the schema specified explicitly" in {
        withClue("We should have received back a connector") {
          HanaConnector.createHanaCloudConnector(testConnection.copy(schema = None), testConnectorConfig).map { connector =>
            connector.value.close()
            succeed
          }
        }
      }

      "fail to connect when the instance id is invalid" in {
        val conn = testConnection.copy(instanceId = "wrong")
        val expectedError =
          DatabaseError(ErrorCategory.Timeout, ErrorName.ConnectionTimeout, timeoutMessage, None, None)

        HanaConnector.createHanaCloudConnector(conn, testConnectorConfig).map { result =>
          result.left.value should beDatabaseErrorEqualWithoutCause(expectedError)
        }
      }

      "fail to connect when the user is invalid" in {
        val conn = testConnection.copy(dbUser = "")
        val expectedError =
          DatabaseError(ErrorCategory.Timeout, ErrorName.ConnectionTimeout, timeoutMessage, None, None)

        HanaConnector.createHanaCloudConnector(conn, testConnectorConfig).map { result =>
          result.left.value should beDatabaseErrorEqualWithoutCause(expectedError)
        }
      }

      "fail to connect when the password is invalid" in {
        val conn = testConnection.copy(dbPassword = "")
        val expectedError =
          DatabaseError(ErrorCategory.Timeout, ErrorName.ConnectionTimeout, timeoutMessage, None, None)

        HanaConnector.createHanaCloudConnector(conn, testConnectorConfig).map { result =>
          result.left.value should beDatabaseErrorEqualWithoutCause(expectedError)
        }
      }
    }
  }
}
