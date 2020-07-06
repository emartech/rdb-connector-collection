package com.emarsys.rdb.connector.snowflake

import java.lang.management.ManagementFactory
import java.util.UUID

import com.emarsys.rdb.connector.common.Models.{MetaData, PoolConfig}
import com.emarsys.rdb.connector.snowflake.SnowflakeConnector.{SnowflakeConnectionConfig, SnowflakeConnectorConfig}
import com.zaxxer.hikari.HikariPoolMXBean
import javax.management.{MBeanServer, ObjectName}
import org.scalatest.{Matchers, WordSpecLike}
import org.scalatestplus.mockito.MockitoSugar
import slick.jdbc.JdbcBackend.Database
import spray.json._

class SnowflakeConnectorSpec extends WordSpecLike with Matchers with MockitoSugar {
  val connectorConfig = SnowflakeConnectorConfig(
    streamChunkSize = 5000,
    configPath = "snowflakedb",
    poolConfig = PoolConfig(2, 100)
  )

  "The SnowflakeConnector's companion" can {
    "#createUrl" should {
      val exampleConnection = SnowflakeConnectionConfig(
        accountName = "olaf",
        warehouseName = "arendelle",
        dbName = "database",
        schemaName = "schema",
        dbUser = "me",
        dbPassword = "secret",
        connectionParams = "?param1=asd"
      )

      "create url from config" in {
        SnowflakeConnector.createUrl(exampleConnection) shouldBe "jdbc:snowflake://olaf.snowflakecomputing.com/?param1=asd"
      }

      "handle missing ? in params" in {
        val exampleWithoutQuestionMark = exampleConnection.copy(connectionParams = "param1=asd")
        SnowflakeConnector.createUrl(exampleWithoutQuestionMark) shouldBe "jdbc:snowflake://olaf.snowflakecomputing.com/?param1=asd"
      }

      "handle empty params" in {
        val emptyParamsExample = exampleConnection.copy(connectionParams = "")
        SnowflakeConnector.createUrl(emptyParamsExample) shouldBe "jdbc:snowflake://olaf.snowflakecomputing.com/"
      }

    }

    "#checkSsl" when {
      "connection params are empty" should {
        "return false" in {
          SnowflakeConnector.isSslDisabled("") shouldBe false
        }
      }

      "connection params do not contain ssl=off" should {
        "return false" in {
          SnowflakeConnector.isSslDisabled("?param1=param&param2=param2&ssl=on") shouldBe false
        }
      }

      "connection params contain ssl=off" should {
        "return true" in {
          SnowflakeConnector.isSslDisabled("?param1=param&ssl=off&param2=param2") shouldBe true
        }
      }
    }

    "#meta" should {
      "return snowflake qouters" in {
        SnowflakeConnector.meta() shouldEqual MetaData(nameQuoter = "\"", valueQuoter = "'", escape = "\\")
      }
    }
  }

  "A SnowflakeConnector instance" can {
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

        val connector   = new SnowflakeConnector(db, connectorConfig, poolName, "public")
        val metricsJson = connector.innerMetrics().parseJson.asJsObject

        metricsJson.fields.size shouldEqual 4
        metricsJson.fields("totalConnections") shouldEqual JsNumber(2)
      }

      "return Json in sad case" in {
        val db          = mock[Database]
        val poolName    = ""
        val connector   = new SnowflakeConnector(db, connectorConfig, poolName, "public")
        val metricsJson = connector.innerMetrics().parseJson.asJsObject
        metricsJson.fields.size shouldEqual 0
      }
    }
  }
}
