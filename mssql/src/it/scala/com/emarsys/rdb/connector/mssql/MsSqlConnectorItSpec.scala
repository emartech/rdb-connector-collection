package com.emarsys.rdb.connector.mssql

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName}
import com.emarsys.rdb.connector.mssql.utils.TestHelper
import com.emarsys.rdb.connector.test.CustomMatchers._
import com.emarsys.rdb.connector.test.util.EitherValues
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class MsSqlConnectorItSpec
    extends TestKit(ActorSystem("connector-it-soec"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with EitherValues {

  val timeout             = 30.seconds

  override def afterAll() = shutdown()

  "MsSqlConnectorItSpec" when {

    val connectorConfig = TestHelper.TEST_CONNECTOR_CONFIG
    val testConnection  = TestHelper.TEST_CONNECTION_CONFIG

    "create connector" should {

      "connect success" in {
        val connectorEither = Await.result(MsSqlConnector.create(testConnection, connectorConfig), timeout)

        connectorEither shouldBe a[Right[_, _]]

        connectorEither.value.close()
      }

      "connect fail when wrong host" in {
        val conn            = testConnection.copy(host = "wrong")
        val connectorEither = Await.result(MsSqlConnector.create(conn, connectorConfig), timeout)

        connectorEither.left.value should haveErrorCategoryAndErrorName(
          ErrorCategory.Timeout,
          ErrorName.ConnectionTimeout
        )
      }

      "connect fail when wrong user" in {
        val conn            = testConnection.copy(dbUser = "")
        val connectorEither = Await.result(MsSqlConnector.create(conn, connectorConfig), timeout)

        connectorEither.left.value should haveErrorCategoryAndErrorName(
          ErrorCategory.FatalQueryExecution,
          ErrorName.AccessDeniedError
        )
      }

      "connect fail when wrong password" in {
        val conn            = testConnection.copy(dbPassword = "")
        val connectorEither = Await.result(MsSqlConnector.create(conn, connectorConfig), timeout)

        connectorEither.left.value should haveErrorCategoryAndErrorName(
          ErrorCategory.FatalQueryExecution,
          ErrorName.AccessDeniedError
        )
      }

      "connect fail when wrong certificate format" in {
        val conn = testConnection.copy(certificate = "")

        val connectorEither = Await.result(MsSqlConnector.create(conn, connectorConfig), timeout)

        connectorEither.left.value should haveErrorCategoryAndErrorName(
          ErrorCategory.FatalQueryExecution,
          ErrorName.SSLError
        )
      }

      "connect fail when wrong certificate" in {
        val conn = testConnection.copy(certificate = """
         |-----BEGIN CERTIFICATE-----
         |MIICljCCAX4CCQDTYHFbvff7nTANBgkqhkiG9w0BAQsFADANMQswCQYDVQQGEwJh
         |czAeFw0xOTExMjcxMDIxMjlaFw0yMDExMjYxMDIxMjlaMA0xCzAJBgNVBAYTAmFz
         |MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyqr8x+Rc+utk9P/0SUHB
         |D5GX3CGRo9OMNUZfCsrNSSTkqXA0ZgQEdIci6tztCbewDLtBLAAXB3EvfoO67V53
         |c8PAfmQklnNvmoWfq4mFTuzoYzhQZZg76yywOAFuTC9gWw5LJVpuB9q9enEl2w+5
         |a4fLTtloc1h3zanEfihZc/a3uJG8zQ2pH1OzejstnLSASNJ20g8KwLu8jqgsMl69
         |6ld6iUsj6zkwS3uQo13yqzu6IcQOEdfpTyi/nIlGT+yL2EKSWeQG78+0TBGLdZfG
         |JDECrW40qgH1jW20/v75hVu4YAnZfnXZuGTFL+IKzB4UjYZ//0qhC88MZQm6y4b3
         |/QIDAQABMA0GCSqGSIb3DQEBCwUAA4IBAQBI2QI++k11eopoMg7tW0WbSkr35aJS
         |THXODnnr40qe3z+ReHNhVTQThScLCYiVNGNW+LzuguDY2zuw0Luwamhlhtn2Uwz5
         |saU74Nnk8v8jGThUYpZ7y9T4uMXaa5dS/Z8b9XBaYnE8cKhrYEIsvNNni7qqoov0
         |EqjKvCPOMnPy8VeIqMuapLWIWn9uVBIZwN43Qth1XAk4j8gTi6s3xOz2/mD22I58
         |fSrBLxB1fBi2XlvwYCwUOgGiF4WzW/LaJO86J+Itdp3/GEpHnpsEFFyg2gdXipqo
         |ZgLlwzUQOYs5tY4CpL544CEzls2ClM3zuIRvobJdQ2YUhvFhQbAYhrfY
         |-----END CERTIFICATE-----
         |
         |""".stripMargin)

        val connectorEither =
          Await.result(MsSqlConnector.create(conn, connectorConfig.copy(trustServerCertificate = false)), timeout)

        connectorEither.left.value should haveErrorCategoryAndErrorName(
          ErrorCategory.Timeout,
          ErrorName.ConnectionTimeout
        )
      }

      "connect fail when ssl disabled" in {
        val conn = testConnection.copy(connectionParams = "encrypt=false")

        val connectorEither = Await.result(MsSqlConnector.create(conn, connectorConfig), timeout)

        connectorEither.left.value should haveErrorCategoryAndErrorName(
          ErrorCategory.FatalQueryExecution,
          ErrorName.SSLError
        )
      }
    }

    "test connection" should {

      "success" in {
        val connectorEither = Await.result(MsSqlConnector.create(testConnection, connectorConfig), timeout)

        connectorEither shouldBe a[Right[_, _]]

        val connector = connectorEither.value

        val result = Await.result(connector.testConnection(), timeout)

        result shouldBe a[Right[_, _]]

        connector.close()
      }

    }

    trait QueryRunnerScope {
      lazy val connectionConfig = testConnection
      lazy val queryTimeout     = 5.second

      def runQuery(q: String): ConnectorResponse[Unit] =
        for {
          Right(connector) <- MsSqlConnector.create(connectionConfig, connectorConfig)
          Right(source)    <- connector.rawSelect(q, limit = None, queryTimeout)
          res              <- sinkOrLeft(source)
          _ = connector.close()
        } yield res

      private def sinkOrLeft[T](source: Source[T, NotUsed]): ConnectorResponse[Unit] =
        source
          .runWith(Sink.ignore)
          .map[Either[DatabaseError, Unit]](_ => Right(()))
          .recover {
            case e: DatabaseError => Left[DatabaseError, Unit](e)
          }
    }

    "Custom error handling" should {
      "recognize query timeouts" in new QueryRunnerScope {
        val result = Await.result(runQuery("waitfor delay '00:00:08.000'; select 1"), timeout)

        result.left.value should haveErrorCategoryAndErrorName(ErrorCategory.Timeout, ErrorName.QueryTimeout)
      }

      "recognize syntax errors" in new QueryRunnerScope {
        val result = Await.result(runQuery("select from table"), timeout)

        result.left.value should haveErrorCategoryAndErrorName(
          ErrorCategory.FatalQueryExecution,
          ErrorName.SqlSyntaxError
        )
      }

      "recognize access denied errors" in new QueryRunnerScope {
        override lazy val connectionConfig = TestHelper.TEST_CONNECTION_CONFIG.copy(
          dbUser = "unprivileged",
          dbPassword = "unprivileged1!"
        )

        val result = Await.result(runQuery("select * from access_denied"), timeout)

        result.left.value should haveErrorCategoryAndErrorName(
          ErrorCategory.FatalQueryExecution,
          ErrorName.AccessDeniedError
        )
      }

      "recognize if a table is not found" in new QueryRunnerScope {
        val result = Await.result(runQuery("select * from a_non_existing_table"), timeout)

        result.left.value should haveErrorCategoryAndErrorName(
          ErrorCategory.FatalQueryExecution,
          ErrorName.TableNotFound
        )
      }
    }
  }
}
