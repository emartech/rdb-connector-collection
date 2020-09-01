package com.emarsys.rdb.connector.common

import com.emarsys.rdb.connector.common.Models.{CommonConnectionReadableData, ConnectionConfig}
import spray.json._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class ConnectionConfigSpec extends AnyWordSpecLike with Matchers {

  case class TestConfig(publicFields: List[String] = Nil, secretFields: List[String] = Nil) extends ConnectionConfig {
    override protected def getPublicFieldsForId               = publicFields
    override protected def getSecretFieldsForId               = secretFields
    override def toCommonFormat: CommonConnectionReadableData = CommonConnectionReadableData("a", "b", "c", "d")
  }

  "ConnectionConfig" should {
    "have a good toString" in {
      val conf   = TestConfig(Nil, Nil)
      val fields = conf.toString.parseJson.asJsObject.fields
      fields.size shouldBe 4
      fields.keySet shouldBe Set("type", "location", "dataset", "user")
    }

    "add public infos to id" in {
      val conf = TestConfig(List("an info", "other data"), Nil)
      conf.getId should include("an info")
      conf.getId should include("other data")
    }

    "add private infos to id with a non recognisable format" in {
      val conf1 = TestConfig(Nil, List("an info", "other data"))
      conf1.getId shouldNot include("an info")
      conf1.getId shouldNot include("other data")

      val conf2 = TestConfig(Nil, List("an info", "other data1"))
      conf1.getId should not equal conf2.getId
    }
  }
}
