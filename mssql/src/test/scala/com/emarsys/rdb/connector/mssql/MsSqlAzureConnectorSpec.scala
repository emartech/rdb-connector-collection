package com.emarsys.rdb.connector.mssql

import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName}
import org.scalatest.{Matchers, WordSpecLike}
import org.scalatestplus.mockito.MockitoSugar

class MsSqlAzureConnectorSpec extends WordSpecLike with Matchers with MockitoSugar {

  "MsSqlAzureConnectorSpec" when {

    "#checkAzureUrl" should {

      "return true if end with .database.windows.net" in {
        MsSqlAzureConnector.checkAzureUrl("hello.database.windows.net") shouldBe true
      }

      "return false if end with only database.windows.net" in {
        MsSqlAzureConnector.checkAzureUrl("database.windows.net") shouldBe false
      }

      "return false" in {
        MsSqlAzureConnector.checkAzureUrl("hello.windows.net") shouldBe false
      }

    }

    "#isErrorRetryable" should {
      Seq(
        DatabaseError(
          ErrorCategory.Unknown,
          ErrorName.Unknown,
          "...was deadlocked on lock resources with another process...",
          None,
          None
        )                                                                                    -> true,
        DatabaseError(ErrorCategory.Unknown, ErrorName.Unknown, "whatever else", None, None) -> false
      ).foreach {
        case (e @ DatabaseError(errorCategory, errorName, message, _, _), expected) =>
          s"return $expected for ${errorCategory}#$errorName - $message" in {
            val connector = new MsSqlConnector(null, null, null)(null)

            connector.isErrorRetryable(e) shouldBe expected
          }
      }
    }
  }
}
