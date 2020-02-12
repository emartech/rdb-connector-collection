package com.emarsys.rdb.connector.mssql

import java.lang.management.ManagementFactory
import java.util.UUID

import com.emarsys.rdb.connector.common.Models._
import com.zaxxer.hikari.HikariPoolMXBean
import javax.management.{MBeanServer, ObjectName}
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpecLike}
import slick.jdbc.SQLServerProfile.api._
import spray.json._

class MsSqlConnectorSpec extends WordSpecLike with Matchers with MockitoSugar {

  "MsSqlConnectorSpec" when {

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

        val connector   = new MsSqlConnector(db, MsSqlConnector.defaultConfig, poolName)
        val metricsJson = connector.innerMetrics().parseJson.asJsObject

        metricsJson.fields.size shouldEqual 4
        metricsJson.fields("totalConnections") shouldEqual JsNumber(2)
      }

      "return Json in sad case" in {
        val db          = mock[Database]
        val poolName    = ""
        val connector   = new MsSqlConnector(db, MsSqlConnector.defaultConfig, poolName)
        val metricsJson = connector.innerMetrics().parseJson.asJsObject
        metricsJson.fields.size shouldEqual 0
      }

    }

    "#meta" should {

      "return mssql qouters" in {
        MsSqlConnector.meta() shouldEqual MetaData(nameQuoter = "\"", valueQuoter = "'", escape = "'")
      }

    }
  }
}
