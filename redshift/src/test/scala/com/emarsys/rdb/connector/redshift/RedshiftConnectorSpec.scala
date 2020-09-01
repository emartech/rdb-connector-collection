package com.emarsys.rdb.connector.redshift

import java.lang.management.ManagementFactory
import java.util.UUID

import com.emarsys.rdb.connector.common.Models.{MetaData, PoolConfig}
import com.emarsys.rdb.connector.redshift.RedshiftConnector.{RedshiftConnectionConfig, RedshiftConnectorConfig}
import com.zaxxer.hikari.HikariPoolMXBean
import javax.management.{MBeanServer, ObjectName}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatestplus.mockito.MockitoSugar
import slick.jdbc.PostgresProfile.api._
import spray.json._

class RedshiftConnectorSpec extends AnyWordSpecLike with Matchers with MockitoSugar {
  val connectorConfig = RedshiftConnectorConfig(
    streamChunkSize = 5000,
    configPath = "redshiftdb",
    poolConfig = PoolConfig(2, 100)
  )

  "RedshiftConnectorTest" when {

    "#createUrl" should {

      val exampleConnection = RedshiftConnectionConfig(
        host = "host",
        port = 123,
        dbName = "database",
        dbUser = "me",
        dbPassword = "secret",
        connectionParams = "?param1=asd"
      )

      "creates url from config" in {
        RedshiftConnector.createUrl(exampleConnection) shouldBe "jdbc:redshift://host:123/database?param1=asd"
      }

      "handle missing ? in params" in {
        val exampleWithoutMark = exampleConnection.copy(connectionParams = "param1=asd")
        RedshiftConnector.createUrl(exampleWithoutMark) shouldBe "jdbc:redshift://host:123/database?param1=asd"
      }

      "handle empty params" in {
        val exampleWithoutMark = exampleConnection.copy(connectionParams = "")
        RedshiftConnector.createUrl(exampleWithoutMark) shouldBe "jdbc:redshift://host:123/database"
      }

    }

    "#checkSsl" should {

      "return false if empty connection params" in {
        RedshiftConnector.isSslDisabled("") shouldBe false
      }

      "return false if not contains ssl=false" in {
        RedshiftConnector.isSslDisabled("?param1=param&param2=param2") shouldBe false
      }

      "return true if contains ssl=false" in {
        RedshiftConnector.isSslDisabled("?param1=param&ssl=false&param2=param2") shouldBe true
      }

    }

    "#meta" should {

      "return redshift qouters" in {
        RedshiftConnector.meta() shouldEqual MetaData(nameQuoter = "\"", valueQuoter = "'", escape = "\\")
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

        val connector   = new RedshiftConnector(db, connectorConfig, poolName, "public")
        val metricsJson = connector.innerMetrics().parseJson.asJsObject

        metricsJson.fields.size shouldEqual 4
        metricsJson.fields("totalConnections") shouldEqual JsNumber(2)
      }

      "return Json in sad case" in {
        val db          = mock[Database]
        val poolName    = ""
        val connector   = new RedshiftConnector(db, connectorConfig, poolName, "public")
        val metricsJson = connector.innerMetrics().parseJson.asJsObject
        metricsJson.fields.size shouldEqual 0
      }

    }

  }
}
