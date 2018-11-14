package com.emarsys.rdb.connector.redshift

import java.lang.management.ManagementFactory
import java.util.UUID

import com.emarsys.rdb.connector.common.models.MetaData
import com.emarsys.rdb.connector.redshift.RedshiftConnector.RedshiftConnectionConfig
import com.zaxxer.hikari.HikariPoolMXBean
import javax.management.{MBeanServer, ObjectName}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpecLike}
import spray.json._

import slick.jdbc.PostgresProfile.api._

class RedshiftConnectorSpec extends WordSpecLike with Matchers with MockitoSugar {

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

      "return true if empty connection params" in {
        RedshiftConnector.checkSsl("") shouldBe true
      }

      "return true if not contains ssl=false" in {
        RedshiftConnector.checkSsl("?param1=param&param2=param2") shouldBe true
      }

      "return false if contains ssl=false" in {
        RedshiftConnector.checkSsl("?param1=param&ssl=false&param2=param2") shouldBe false
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

        val connector   = new RedshiftConnector(db, RedshiftConnector.defaultConfig, poolName, "public")
        val metricsJson = connector.innerMetrics().parseJson.asJsObject

        metricsJson.fields.size shouldEqual 4
        metricsJson.fields("totalConnections") shouldEqual JsNumber(2)
      }

      "return Json in sad case" in {
        val db          = mock[Database]
        val poolName    = ""
        val connector   = new RedshiftConnector(db, RedshiftConnector.defaultConfig, poolName, "public")
        val metricsJson = connector.innerMetrics().parseJson.asJsObject
        metricsJson.fields.size shouldEqual 0
      }

    }

  }
}
