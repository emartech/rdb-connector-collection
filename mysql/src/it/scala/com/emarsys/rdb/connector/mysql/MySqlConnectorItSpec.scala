package com.emarsys.rdb.connector.mysql

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import cats.data.EitherT
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors._
import com.emarsys.rdb.connector.mysql.utils.TestHelper
import com.emarsys.rdb.connector.mysql.MySqlConnector.MySqlConnectorConfig
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class MySqlConnectorItSpec
    extends TestKit(ActorSystem("mysql-connector-it-test"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  implicit val mat: ActorMaterializer = ActorMaterializer()
  override def afterAll: Unit = shutdown()

  "MySqlConnectorItSpec" when {

    val testConnection = TestHelper.TEST_CONNECTION_CONFIG

    "create connector" should {

      "connect success" in {
        val connectorEither = Await.result(MySqlConnector.create(testConnection, MySqlConnectorConfig("mysqldb", false)), 5.seconds)

        connectorEither shouldBe a[Right[_, _]]

        connectorEither.right.get.close()
      }

      "connect fail when ssl disabled" ignore {
        val conn = testConnection.copy(
          connectionParams = "useSSL=false"
        )
        val connectorEither = Await.result(MySqlConnector.create(conn, MySqlConnectorConfig(
          configPath = "mysqldb",
          verifyServerCertificate = false
        )), 5.seconds)

        connectorEither shouldBe Left(ConnectionConfigError("SSL Error"))
      }


      "connect fail when wrong certificate" in {
        val conn            = testConnection.copy(certificate = "")
        val connectorEither = Await.result(MySqlConnector.create(conn), 5.seconds)

        connectorEither shouldBe Left(ConnectionConfigError("Wrong SSL cert format"))
      }

      "connect fail when wrong host" in {
        val conn            = testConnection.copy(host = "wrong")
        val connectorEither = Await.result(MySqlConnector.create(conn), 5.seconds)

        connectorEither shouldBe a[Left[_, _]]
        connectorEither.left.get shouldBe an[ConnectionTimeout]
      }

      "connect fail when wrong user" in {
        val conn            = testConnection.copy(dbUser = "")
        val connectorEither = Await.result(MySqlConnector.create(conn), 5.seconds)

        connectorEither shouldBe a[Left[_, _]]
        connectorEither.left.get shouldBe an[ConnectionTimeout]
      }

      "connect fail when wrong password" in {
        val conn            = testConnection.copy(dbPassword = "")
        val connectorEither = Await.result(MySqlConnector.create(conn), 5.seconds)

        connectorEither shouldBe a[Left[_, _]]
        connectorEither.left.get shouldBe an[ConnectionTimeout]
      }

    }

    "test connection" should {

      "return success" in {
        val connectorEither = Await.result(MySqlConnector.create(testConnection, MySqlConnectorConfig(
          configPath = "mysqldb",
          verifyServerCertificate = false
        )), 5.seconds)

        connectorEither shouldBe a[Right[_, _]]

        val connector = connectorEither.right.get

        val result = Await.result(connector.testConnection(), 5.seconds)

        result shouldBe a[Right[_, _]]

        connector.close()
      }

    }

    "custom error handling" should {
      import cats.instances.future._
      def runQuery(q: String): ConnectorResponse[Unit] =
        (for {
          connector <- EitherT(MySqlConnector.create(testConnection, MySqlConnectorConfig(
            configPath = "mysqldb",
            verifyServerCertificate = false
          )))
          source    <- EitherT(connector.rawSelect(q, limit = None, timeout = 1.second))
          res              <- EitherT(sinkOrLeft(source))
          _ = connector.close()
        } yield res).value

      def sinkOrLeft[T](source: Source[T, NotUsed]): ConnectorResponse[Unit] =
        source
          .runWith(Sink.ignore)
          .map[Either[ConnectorError, Unit]](_ => Right(()))
          .recover {
            case e: ConnectorError => Left[ConnectorError, Unit](e)
          }

      "recognize syntax errors" in {
        val result = Await.result(runQuery("select from table"), 10.second)

        result shouldBe a[Left[_, _]]
        result.left.get shouldBe an[SqlSyntaxError]
      }

      "recognize access denied errors" in {
        val result = Await.result(runQuery("select * from information_schema.innodb_metrics"), 10.second)

        result shouldBe a[Left[_, _]]
        result.left.get shouldBe an[AccessDeniedError]
      }
    }
  }

}
