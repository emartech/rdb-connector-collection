package com.emarsys.rdb.connector.mssql

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class MsSqlConnectorHelperSpec extends AnyWordSpecLike with Matchers {

  "MsSqlConnectorHelperSpec" when {

    object MsSqlConnectorHelper extends MsSqlConnectorHelper

    "#createUrl" should {

      "creates url from config" in {
        MsSqlConnectorHelper.createUrl("host", 123, "database", ";param1=asd") shouldBe "jdbc:sqlserver://host:123;databaseName=database;param1=asd"
      }

      "handle missing ; in params" in {
        MsSqlConnectorHelper.createUrl("host", 123, "database", "param1=asd") shouldBe "jdbc:sqlserver://host:123;databaseName=database;param1=asd"
      }

      "handle empty params" in {
        MsSqlConnectorHelper.createUrl("host", 123, "database", "") shouldBe "jdbc:sqlserver://host:123;databaseName=database"
      }
    }

    "#isSslDisabledOrTamperedWith" should {

      "return false if empty connection params" in {
        MsSqlConnectorHelper.isSslDisabledOrTamperedWith("") shouldBe false
      }

      "return false if not contains ssl config" in {
        MsSqlConnectorHelper.isSslDisabledOrTamperedWith("?param1=param&param2=param2") shouldBe false
      }

      "return true if contains encrypt=false" in {
        MsSqlConnectorHelper.isSslDisabledOrTamperedWith("?param1=param&encrypt=false&param2=param2") shouldBe true
      }

      "return true if contains trustServerCertificate=true" in {
        MsSqlConnectorHelper.isSslDisabledOrTamperedWith("?param1=param&trustServerCertificate=true&param2=param2") shouldBe true
      }

      "return true if contains trustStore=" in {
        MsSqlConnectorHelper.isSslDisabledOrTamperedWith("?param1=param&trustStore=asd&param2=param2") shouldBe true
      }

    }
  }
}
