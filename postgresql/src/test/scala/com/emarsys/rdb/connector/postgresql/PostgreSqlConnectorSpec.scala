package com.emarsys.rdb.connector.postgresql

import java.lang.management.ManagementFactory
import java.util.UUID

import com.emarsys.rdb.connector.common.Models.{MetaData, PoolConfig}
import com.emarsys.rdb.connector.postgresql.PostgreSqlConnector.{PostgreSqlConnectionConfig, PostgreSqlConnectorConfig}
import com.zaxxer.hikari.HikariPoolMXBean
import javax.management.{MBeanServer, ObjectName}
import org.scalatest.{Matchers, WordSpecLike}
import org.scalatestplus.mockito.MockitoSugar
import slick.jdbc.PostgresProfile.api._
import spray.json._

class PostgreSqlConnectorSpec extends WordSpecLike with Matchers with MockitoSugar {

  lazy val connectorConfig = PostgreSqlConnectorConfig(
    streamChunkSize = 5000,
    configPath = "postgredb",
    sslMode = "verify-ca",
    poolConfig = PoolConfig(2, 100)
  )

  "PostgreSqlConnectorSpec" when {

    "#createUrl" should {

      val exampleConnection = PostgreSqlConnectionConfig(
        host = "host",
        port = 123,
        dbName = "database",
        dbUser = "me",
        dbPassword = "secret",
        certificate = "cert",
        connectionParams = "?param1=asd"
      )

      "creates url from config" in {
        PostgreSqlConnector.createUrl(exampleConnection) shouldBe "jdbc:postgresql://host:123/database?param1=asd"
      }

      "handle missing ? in params" in {
        val exampleWithoutMark = exampleConnection.copy(connectionParams = "param1=asd")
        PostgreSqlConnector.createUrl(exampleWithoutMark) shouldBe "jdbc:postgresql://host:123/database?param1=asd"
      }

      "handle empty params" in {
        val exampleWithoutMark = exampleConnection.copy(connectionParams = "")
        PostgreSqlConnector.createUrl(exampleWithoutMark) shouldBe "jdbc:postgresql://host:123/database"
      }

    }

    "#isSslDisabledOrTamperedWith" should {

      "return false if empty connection params" in {
        PostgreSqlConnector.isSslDisabledOrTamperedWith("") shouldBe false
      }

      "return false if not contains ssl=false or sslrootcert or sslmode" in {
        PostgreSqlConnector.isSslDisabledOrTamperedWith("?param1=param&param2=param2") shouldBe false
      }

      "return true if contains ssl=false" in {
        PostgreSqlConnector.isSslDisabledOrTamperedWith("?param1=param&ssl=false&param2=param2") shouldBe true
      }

      "return true if contains sslrootcert" in {
        PostgreSqlConnector.isSslDisabledOrTamperedWith("?param1=param&sslrootcert=false&param2=param2") shouldBe true
      }

      "return true if contains sslmode" in {
        PostgreSqlConnector.isSslDisabledOrTamperedWith("?param1=param&sslmode=false&param2=param2") shouldBe true
      }

    }

    "#meta" should {

      "return postgresql qouters" in {
        PostgreSqlConnector.meta() shouldEqual MetaData(nameQuoter = "\"", valueQuoter = "'", escape = "\\")
      }

    }

    "#innerMetrics" should {

      implicit val executionContext = concurrent.ExecutionContext.Implicits.global

      "return Json in happy case" in {
        val mxPool = new HikariPoolMXBean {
          override def resumePool(): Unit = ???

          override def softEvictConnections(): Unit = ???

          override def getActiveConnections: Int = 4

          override def getThreadsAwaitingConnection: Int = 3

          override def suspendPool(): Unit = ???

          override def getTotalConnections: Int = 2

          override def getIdleConnections: Int = 1
        }

        val poolName = UUID.randomUUID.toString
        val db       = mock[Database]

        val mbs: MBeanServer      = ManagementFactory.getPlatformMBeanServer()
        val mBeanName: ObjectName = new ObjectName(s"com.zaxxer.hikari:type=Pool ($poolName)")
        mbs.registerMBean(mxPool, mBeanName)

        val connector   = new PostgreSqlConnector(db, connectorConfig, poolName, "public")
        val metricsJson = connector.innerMetrics().parseJson.asJsObject

        metricsJson.fields.size shouldEqual 4
        metricsJson.fields("totalConnections") shouldEqual JsNumber(2)
      }

      "return Json in sad case" in {
        val db          = mock[Database]
        val poolName    = ""
        val connector   = new PostgreSqlConnector(db, connectorConfig, poolName, "public")
        val metricsJson = connector.innerMetrics().parseJson.asJsObject
        metricsJson.fields.size shouldEqual 0
      }

    }

  }
}
