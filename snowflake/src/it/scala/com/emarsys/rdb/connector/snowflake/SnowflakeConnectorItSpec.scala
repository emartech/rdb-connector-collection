package com.emarsys.rdb.connector.snowflake

import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName}
import com.emarsys.rdb.connector.snowflake.utils.TestHelper
import com.emarsys.rdb.connector.test.CustomMatchers._
import org.scalatest.{EitherValues, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._

class SnowflakeConnectorItSpec extends WordSpecLike with EitherValues with Matchers {
  "The SnowflakeConnector's companion" can {
    implicit val executionContext = scala.concurrent.ExecutionContext.Implicits.global
    val timeout                   = 8.seconds

    "create connector" when {
      "ssl is disabled" should {
        "return an SSLError" in {
          var connectionParams = TestHelper.TEST_CONNECTION_CONFIG.connectionParams
          if (!connectionParams.isEmpty) {
            connectionParams += "&"
          }
          connectionParams += "ssl=off"

          val badConnection = TestHelper.TEST_CONNECTION_CONFIG.copy(connectionParams = connectionParams)
          val result        = Await.result(SnowflakeConnector.create(badConnection, TestHelper.TEST_CONNECTOR_CONFIG), timeout)
          result.left.value should beDatabaseErrorEqualWithoutCause(
            DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SSLError, "SSL is disabled")
          )
        }
      }

      "everything is fine" should {
        "connect without a problem" in {
          val connectorEither =
            Await.result(
              SnowflakeConnector.create(TestHelper.TEST_CONNECTION_CONFIG, TestHelper.TEST_CONNECTOR_CONFIG),
              timeout
            )

          connectorEither shouldBe a[Right[_, _]]
          connectorEither.right.value.close()
        }
      }
    }
  }
}
