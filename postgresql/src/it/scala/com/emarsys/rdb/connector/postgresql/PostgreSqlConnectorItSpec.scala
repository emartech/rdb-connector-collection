package com.emarsys.rdb.connector.postgresql

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors._
import com.emarsys.rdb.connector.postgresql.utils.TestHelper
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._

class PostgreSqlConnectorItSpec
    extends TestKit(ActorSystem("postgre-connector-it-spec"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  implicit val mat      = ActorMaterializer()
  override def afterAll = TestKit.shutdownActorSystem(system)

  "PostgreSqlConnector" when {

    implicit val executionContext = scala.concurrent.ExecutionContext.Implicits.global
    val defaultConnection         = TestHelper.TEST_CONNECTION_CONFIG

    "create connector" should {

      "connect success" in {
        val connectorEither = Await.result(PostgreSqlConnector.create(defaultConnection), 5.seconds)

        connectorEither shouldBe a[Right[_, _]]

        connectorEither.right.get.close()
      }

      "connect fail when ssl disabled" in {
        val conn            = defaultConnection.copy(connectionParams = "ssl=false")
        val connectorEither = Await.result(PostgreSqlConnector.create(conn), 5.seconds)

        connectorEither shouldBe Left(ConnectionConfigError("SSL Error"))
      }

      "connect fail when sslrootcert modified" in {
        val conn            = defaultConnection.copy(connectionParams = "sslrootcert=/root.crt")
        val connectorEither = Await.result(PostgreSqlConnector.create(conn), 5.seconds)

        connectorEither shouldBe Left(ConnectionConfigError("SSL Error"))
      }

      "connect fail when sslmode modified" in {
        val conn            = defaultConnection.copy(connectionParams = "sslmode=disable")
        val connectorEither = Await.result(PostgreSqlConnector.create(conn), 5.seconds)

        connectorEither shouldBe Left(ConnectionConfigError("SSL Error"))
      }

      "connect fail when wrong certificate" in {
        val conn            = defaultConnection.copy(certificate = "")
        val connectorEither = Await.result(PostgreSqlConnector.create(conn), 5.seconds)

        connectorEither shouldBe a[Left[_, _]]
        connectorEither.left.get shouldBe a[ConnectionError]
      }

      "connect fail when wrong host" in {
        val conn            = defaultConnection.copy(host = "wrong")
        val connectorEither = Await.result(PostgreSqlConnector.create(conn), 5.seconds)

        connectorEither shouldBe a[Left[_, _]]
        connectorEither.left.get shouldBe a[ConnectionError]
      }

      "connect fail when wrong user" in {
        val conn            = defaultConnection.copy(dbUser = "")
        val connectorEither = Await.result(PostgreSqlConnector.create(conn), 5.seconds)

        connectorEither shouldBe a[Left[_, _]]
        connectorEither.left.get shouldBe a[ConnectionError]
      }

      "connect fail when wrong password" in {
        val conn            = defaultConnection.copy(dbPassword = "")
        val connectorEither = Await.result(PostgreSqlConnector.create(conn), 5.seconds)

        connectorEither shouldBe a[Left[_, _]]
        connectorEither.left.get shouldBe a[ConnectionError]
      }

    }

    "#testConnection" should {

      "success" in {
        val Right(connection) = Await.result(PostgreSqlConnector.create(defaultConnection), 3.seconds)
        val result            = Await.result(connection.testConnection(), 3.seconds)
        result shouldBe Right(())
        connection.close()
      }

    }

    trait QueryRunnerScope {
      lazy val connectionConfig = defaultConnection
      lazy val queryTimeout     = 5.second

      def runQuery(q: String): ConnectorResponse[Unit] =
        for {
          Right(connector) <- PostgreSqlConnector.create(connectionConfig)
          Right(source)    <- connector.rawSelect(q, limit = None, queryTimeout)
          res              <- sinkOrLeft(source)
          _ <- connector.close()
        } yield res

      def sinkOrLeft[T](source: Source[T, NotUsed]): ConnectorResponse[Unit] =
        source
          .runWith(Sink.ignore)
          .map[Either[ConnectorError, Unit]](_ => Right(()))
          .recover {
            case e: ConnectorError => Left[ConnectorError, Unit](e)
          }
    }

    "custom error handling" should {
      "recognize syntax errors" in new QueryRunnerScope {
        val result = Await.result(runQuery("select from table"), 1.second)

        result shouldBe a[Left[_, _]]
        result.left.get shouldBe an[SqlSyntaxError]
      }

      "recognize access denied errors" in new QueryRunnerScope {
        override lazy val connectionConfig = TestHelper.TEST_CONNECTION_CONFIG.copy(
          dbUser = "unprivileged",
          dbPassword = "unprivilegedpw"
        )
        val result = Await.result(runQuery("select * from access_denied"), 1.second)

        result shouldBe a[Left[_, _]]
        result.left.get shouldBe an[AccessDeniedError]
      }

      "recognize query timeouts" in new QueryRunnerScope {
        val result = Await.result(runQuery("select pg_sleep(6)"), 6.second)

        result shouldBe a[Left[_, _]]
        result.left.get shouldBe a[QueryTimeout]
      }

      "recognize if a table is not found" in new QueryRunnerScope {
        val result = Await.result(runQuery("select * from a_non_existing_table"), 1.second)

        result shouldBe a[Left[_, _]]
        result.left.get shouldBe a[TableNotFound]
      }
    }
  }
}
