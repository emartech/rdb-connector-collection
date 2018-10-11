package com.emarsys.rdb.connector.common

import com.emarsys.rdb.connector.common.models.{CommonConnectionReadableData, ConnectionConfig}
import org.scalatest.{Matchers, WordSpecLike}
import spray.json._

class ConnectionConfigSpec extends WordSpecLike with Matchers {

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
