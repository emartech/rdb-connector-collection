package com.emarsys.rdb.connector.bigquery.util

import org.scalatest.{Matchers, WordSpecLike}

class PrivateKeyParserSpec extends WordSpecLike with Matchers {

  "PrivateKeyParserSpec" should {
    "return key" in {
      val key =
        """-----BEGIN PRIVATE KEY-----
          |aGVsbG8=
          |-----END PRIVATE KEY-----
          |""".stripMargin

      PrivateKeyParser.parse(key).map(_.toSeq) shouldEqual Some(Seq(104, 101, 108, 108, 111))
    }

    "return none if wrong prefix" in {
      val key =
        """-BEGIN PRIVATE KEY-----
          |aGVsbG8=
          |-----END PRIVATE KEY-----
          |""".stripMargin

      PrivateKeyParser.parse(key) shouldEqual None
    }

    "return none if wrong postfix" in {
      val key =
        """-----BEGIN PRIVATE KEY-----
          |aGVsbG8=
          |--END PRIVATE KEY-----
          |""".stripMargin

      PrivateKeyParser.parse(key) shouldEqual None
    }

    "return none if wrong encode" in {
      val key =
        """-----BEGIN PRIVATE KEY-----
          |asd
          |-----END PRIVATE KEY-----
        """.stripMargin

      PrivateKeyParser.parse(key) shouldEqual None
    }

    "return none if empty code" in {
      val key =
        """-----BEGIN PRIVATE KEY-----
          |-----END PRIVATE KEY-----
        """.stripMargin

      PrivateKeyParser.parse(key) shouldEqual None
    }
  }
}
