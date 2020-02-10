package com.emarsys.rdb.connector.common

import com.emarsys.rdb.connector.common.Models.{CommonConnectionReadableData, ConnectionConfig}
import org.scalatest.{Matchers, WordSpecLike}
import spray.json._

class ConnectionConfigSpec extends WordSpecLike with Matchers {

  "ConnectionConfig" should {
    "have a good toString" in {
      val conf = new ConnectionConfig {
        override protected def getPublicFieldsForId = List()
        override protected def getSecretFieldsForId = List()
        override def toCommonFormat: CommonConnectionReadableData = CommonConnectionReadableData("a", "b", "c", "d")
      }
      val fields = conf.toString.parseJson.asJsObject.fields
      fields.size shouldBe 4
      fields.keySet shouldBe Set("type", "location", "dataset", "user")
    }

    "add public infos to id" in {
      val conf = new ConnectionConfig {
        override protected def getPublicFieldsForId = List("an info", "other data")
        override protected def getSecretFieldsForId = List()
        override def toCommonFormat: CommonConnectionReadableData = CommonConnectionReadableData("a", "b", "c", "d")
      }
      conf.getId should include("an info")
      conf.getId should include("other data")
    }

    "add private infos to id with a non recognisable format" in {
      val conf1 = new ConnectionConfig {
        override protected def getPublicFieldsForId = List()
        override protected def getSecretFieldsForId = List("an info", "other data")
        override def toCommonFormat: CommonConnectionReadableData = CommonConnectionReadableData("a", "b", "c", "d")
      }
      conf1.getId shouldNot include("an info")
      conf1.getId shouldNot include("other data")

      val conf2 = new ConnectionConfig {
        override protected def getPublicFieldsForId = List()
        override protected def getSecretFieldsForId = List("an info", "other data1")
        override def toCommonFormat: CommonConnectionReadableData = CommonConnectionReadableData("a", "b", "c", "d")
      }
      conf1.getId should not equal conf2.getId
    }
  }

}
