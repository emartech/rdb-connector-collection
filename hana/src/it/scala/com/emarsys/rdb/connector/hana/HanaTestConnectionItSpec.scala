package com.emarsys.rdb.connector.hana

import com.emarsys.rdb.connector.hana.utils.BaseDbSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpecLike

import scala.concurrent.ExecutionContext

class HanaTestConnectionItSpec extends AsyncWordSpecLike with Matchers with BaseDbSpec {
  implicit val excon: ExecutionContext = ec

  "#test" should {
    "test the connection" in {
      connector.testConnection() map { result =>
        result shouldBe Right(())
      }
    }
  }
}
