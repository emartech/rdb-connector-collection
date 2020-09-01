package com.emarsys.rdb.connector.mssql

import java.lang.management.ManagementFactory
import java.util.UUID

import com.emarsys.rdb.connector.common.Models.{MetaData, PoolConfig}
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName}
import com.emarsys.rdb.connector.mssql.MsSqlConnector.MsSqlConnectorConfig
import com.zaxxer.hikari.HikariPoolMXBean
import javax.management.{MBeanServer, ObjectName}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatestplus.mockito.MockitoSugar
import slick.jdbc.SQLServerProfile.api._
import spray.json._

class MsSqlConnectorSpec extends AnyWordSpecLike with Matchers with MockitoSugar {

  val defaultConfig = MsSqlConnectorConfig(
    configPath = "mssqldb",
    trustServerCertificate = true,
    poolConfig = PoolConfig(2, 100)
  )

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

        val connector   = new MsSqlConnector(db, defaultConfig, poolName)
        val metricsJson = connector.innerMetrics().parseJson.asJsObject

        metricsJson.fields.size shouldEqual 4
        metricsJson.fields("totalConnections") shouldEqual JsNumber(2)
      }

      "return Json in sad case" in {
        val db          = mock[Database]
        val poolName    = ""
        val connector   = new MsSqlConnector(db, defaultConfig, poolName)
        val metricsJson = connector.innerMetrics().parseJson.asJsObject
        metricsJson.fields.size shouldEqual 0
      }

    }

    "#meta" should {

      "return mssql qouters" in {
        MsSqlConnector.meta() shouldEqual MetaData(nameQuoter = "\"", valueQuoter = "'", escape = "'")
      }

    }

    "#isErrorRetryable" should {
      Seq(
        DatabaseError(
          ErrorCategory.Unknown,
          ErrorName.Unknown,
          "...was deadlocked on lock resources with another process...",
          None,
          None
        )                                                                                    -> true,
        DatabaseError(ErrorCategory.Unknown, ErrorName.Unknown, "whatever else", None, None) -> false
      ).foreach {
        case (e @ DatabaseError(errorCategory, errorName, message, _, _), expected) =>
          s"return $expected for ${errorCategory}#$errorName - $message" in {
            val connector = new MsSqlConnector(null, null, null)(null)

            connector.isErrorRetryable(e) shouldBe expected
          }
      }
    }
  }
}
