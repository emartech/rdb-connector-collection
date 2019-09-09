package com.emarsys.rdb.connector.mssql

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors._
import com.emarsys.rdb.connector.mssql.utils.TestHelper
import com.emarsys.rdb.connector.test.CustomMatchers._
import org.scalatest.{BeforeAndAfterAll, EitherValues, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class MsSqlConnectorItSpec
    extends TestKit(ActorSystem("connector-it-soec"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with EitherValues {

  implicit val mat      = ActorMaterializer()
  override def afterAll = TestKit.shutdownActorSystem(system)

  private val timeout = 10.seconds

  "MsSqlConnectorItSpec" when {

    val testConnection = TestHelper.TEST_CONNECTION_CONFIG

    "create connector" should {

      "connect success" in {
        val connectorEither = Await.result(MsSqlConnector.create(testConnection), timeout)

        connectorEither shouldBe a[Right[_, _]]

        connectorEither.right.get.close()
      }

      "connect fail when wrong host" in {
        val conn            = testConnection.copy(host = "wrong")
        val connectorEither = Await.result(MsSqlConnector.create(conn), timeout)

        val result = connectorEither.left.value.asInstanceOf[DatabaseError]
        result.copy(message = "") should beDatabaseErrorEqualWithoutCause(
          DatabaseError(ErrorCategory.Timeout, ErrorName.ConnectionTimeout, "", None, None)
        )
        result.message should include("Connection is not available, request timed out after")
        result.cause shouldBe defined
      }

      "connect fail when wrong user" in {
        val conn            = testConnection.copy(dbUser = "")
        val connectorEither = Await.result(MsSqlConnector.create(conn), timeout)

        val result = connectorEither.left.value.asInstanceOf[DatabaseError]
        result.copy(message = "") should beDatabaseErrorEqualWithoutCause(
          DatabaseError(ErrorCategory.Timeout, ErrorName.ConnectionTimeout, "", None, None)
        )
        result.message should include("Connection is not available, request timed out after")
        result.cause shouldBe defined
      }

      "connect fail when wrong password" in {
        val conn            = testConnection.copy(dbPassword = "")
        val connectorEither = Await.result(MsSqlConnector.create(conn), timeout)

        val result = connectorEither.left.value.asInstanceOf[DatabaseError]
        result.copy(message = "") should beDatabaseErrorEqualWithoutCause(
          DatabaseError(ErrorCategory.Timeout, ErrorName.ConnectionTimeout, "", None, None)
        )
        result.message should include("Connection is not available, request timed out after")
        result.cause shouldBe defined
      }

      "connect fail when wrong certificate" in {
        val conn            = testConnection.copy(certificate = "")
        val connectorEither = Await.result(MsSqlConnector.create(conn), timeout)

        val result = connectorEither.left.value.asInstanceOf[DatabaseError]
        result should beDatabaseErrorEqualWithoutCause(
          DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SSLError, "Wrong SSL cert format", None, None)
        )
        result.cause.map(_.toString) shouldBe Some(
          "java.security.cert.CertificateException: Could not parse certificate: java.io.IOException: Empty input"
        )
      }

      "connect fail when ssl disabled" in {
        val conn            = testConnection.copy(connectionParams = "encrypt=false")
        val connectorEither = Await.result(MsSqlConnector.create(conn), timeout)

        val result = connectorEither.left.value.asInstanceOf[DatabaseError]
        result should beDatabaseErrorEqualWithoutCause(
          DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SSLError, "SSL is disabled", None, None)
        )
        result.cause shouldBe None
      }

    }

    "test connection" should {

      "success" in {
        val connectorEither = Await.result(MsSqlConnector.create(testConnection), timeout)

        connectorEither shouldBe a[Right[_, _]]

        val connector = connectorEither.right.get

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
          Right(connector) <- MsSqlConnector.create(connectionConfig)
          Right(source)    <- connector.rawSelect(q, limit = None, queryTimeout)
          res              <- sinkOrLeft(source)
          _ = connector.close()
        } yield res

      def sinkOrLeft[T](source: Source[T, NotUsed]): ConnectorResponse[Unit] =
        source
          .runWith(Sink.ignore)
          .map[Either[ConnectorError, Unit]](_ => Right(()))
          .recover {
            case e: ConnectorError => Left[ConnectorError, Unit](e)
          }
    }

    "Custom error handling" should {
      "recognize query timeouts" in new QueryRunnerScope {
        val result = Await.result(runQuery("waitfor delay '00:00:06.000'; select 1"), 6.second)

        result.left.value should beDatabaseErrorEqualWithoutCause(
          DatabaseError(ErrorCategory.Timeout, ErrorName.QueryTimeout, "", None, None)
        )
      }

      "recognize syntax errors" in new QueryRunnerScope {
        val result = Await.result(runQuery("select from table"), 1.second)

        result.left.value should beDatabaseErrorEqualWithoutCause(
          DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SqlSyntaxError, "", None, None)
        )
      }

      "recognize access denied errors" in new QueryRunnerScope {
        override lazy val connectionConfig = TestHelper.TEST_CONNECTION_CONFIG.copy(
          dbUser = "unprivileged",
          dbPassword = "unprivileged1!"
        )
        val result = Await.result(runQuery("select * from access_denied"), 1.second)

        result.left.value should beDatabaseErrorEqualWithoutCause(
          DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.AccessDeniedError, "", None, None)
        )
      }

      "recognize if a table is not found" in new QueryRunnerScope {
        val result = Await.result(runQuery("select * from a_non_existing_table"), 1.second)

        result.left.value should beDatabaseErrorEqualWithoutCause(
          DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.TableNotFound, "", None, None)
        )
      }
    }
  }
}
