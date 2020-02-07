package com.emarsys.rdb.connector.common

import com.emarsys.rdb.connector.common.Models.{CommonConnectionReadableData, ConnectionConfig}
import spray.json._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class ConnectionConfigSpec extends AnyWordSpecLike with Matchers {

  "ConnectionConfig" should {
    "have a good toString" in {
      val conf = new ConnectionConfig {
        override def toCommonFormat: CommonConnectionReadableData = CommonConnectionReadableData("a", "b", "c", "d")
      }
      val fields = conf.toString.parseJson.asJsObject.fields
      fields.size shouldBe 4
      fields.keySet shouldBe Set("type", "location", "dataset", "user")
    }
  }

}
