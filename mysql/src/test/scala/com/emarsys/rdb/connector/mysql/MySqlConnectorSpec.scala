package com.emarsys.rdb.connector.mysql

import java.lang.management.ManagementFactory
import java.sql.SQLTransientException
import java.util.UUID

import com.emarsys.rdb.connector.common.Models.MetaData
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName}
import com.emarsys.rdb.connector.mysql.MySqlConnector.MySqlConnectionConfig
import com.zaxxer.hikari.HikariPoolMXBean
import javax.management.{MBeanServer, ObjectName}
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpec}
import slick.jdbc.MySQLProfile.api._
import spray.json._

class MySqlConnectorSpec extends WordSpec with Matchers with MockitoSugar {

  "MySqlConnector" when {

    val exampleConnection = MySqlConnectionConfig(
      host = "host",
      port = 123,
      dbName = "database",
      dbUser = "me",
      dbPassword = "secret",
      certificate = "cert",
      connectionParams = "?param1=asd",
      replicaConfig = None
    )

    "#isErrorRetryable" should {

      Seq(
        DatabaseError(ErrorCategory.Transient, ErrorName.TransientDbError, "whatever", None, None) -> true,
        DatabaseError(ErrorCategory.Timeout, ErrorName.ConnectionTimeout, "whatever", None, None)  -> true,
        DatabaseError(ErrorCategory.Timeout, ErrorName.QueryTimeout, "whatever", None, None)       -> false,
        DatabaseError(ErrorCategory.Timeout, ErrorName.CompletionTimeout, "whatever", None, None)  -> false,
        DatabaseError(ErrorCategory.Unknown, ErrorName.Unknown, "Deadlock detected", None, None)   -> true,
        DatabaseError(ErrorCategory.Unknown, ErrorName.Unknown, "whatever else", None, None)       -> false
      ).foreach {
        case (e @ DatabaseError(errorCategory, errorName, message, _, _), expected) =>
          s"return $expected for ${errorCategory}#$errorName - $message" in {
            val connector = new MySqlConnector(null, null, null)(null)

            connector.isErrorRetryable(e) shouldBe expected
          }
      }

      Seq(
        new SQLTransientException()    -> true,
        new Exception("whatever else") -> false
      ).foreach {
        case (e, expected) =>
          s"return $expected for ${e.getClass.getSimpleName}" in {
            val connector = new MySqlConnector(null, null, null)(null)

            connector.isErrorRetryable(e) shouldBe expected
          }
      }
    }

    "#createUrl" should {

      "creates url from config" in {
        MySqlConnector.createJdbcUrl(exampleConnection) shouldBe "jdbc:mysql://host:123/database?param1=asd"
      }

      "handle missing ? in params" in {
        val exampleWithoutMark = exampleConnection.copy(connectionParams = "param1=asd")
        MySqlConnector.createJdbcUrl(exampleWithoutMark) shouldBe "jdbc:mysql://host:123/database?param1=asd"
      }

      "handle empty params" in {
        val exampleWithoutMark = exampleConnection.copy(connectionParams = "")
        MySqlConnector.createJdbcUrl(exampleWithoutMark) shouldBe "jdbc:mysql://host:123/database"
      }
    }

    "#meta" should {

      "return mysql qouters" in {
        MySqlConnector.meta() shouldEqual MetaData(nameQuoter = "`", valueQuoter = "'", escape = "\\")
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

        val connector   = new MySqlConnector(db, MySqlConnector.defaultConfig, poolName)
        val metricsJson = connector.innerMetrics().parseJson.asJsObject

        metricsJson.fields.size shouldEqual 4
        metricsJson.fields("totalConnections") shouldEqual JsNumber(2)
      }

      "return Json in sad case" in {
        val db          = mock[Database]
        val poolName    = ""
        val connector   = new MySqlConnector(db, MySqlConnector.defaultConfig, poolName)
        val metricsJson = connector.innerMetrics().parseJson.asJsObject
        metricsJson.fields.size shouldEqual 0
      }
    }
  }
}
