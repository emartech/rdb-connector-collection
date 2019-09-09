package com.emarsys.rdb.connector.redshift

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.redshift.utils.TestHelper
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName, ConnectorError}
import org.scalatest.{BeforeAndAfterAll, EitherValues, Matchers, WordSpecLike}
import com.emarsys.rdb.connector.test.CustomMatchers._

import scala.concurrent.Await
import scala.concurrent.duration._

class RedshiftConnectorItSpec
    extends TestKit(ActorSystem("connector-it-test"))
    with WordSpecLike
    with Matchers
    with EitherValues
    with BeforeAndAfterAll {
  implicit val mat              = ActorMaterializer()
  override def afterAll(): Unit = shutdown()

  "RedshiftConnector" when {

    implicit val executionContext = scala.concurrent.ExecutionContext.Implicits.global
    val timeout                   = 8.seconds

    "create connector" should {

      "return error if not use ssl" in {
        var connectionParams = TestHelper.TEST_CONNECTION_CONFIG.connectionParams
        if (!connectionParams.isEmpty) {
          connectionParams += "&"
        }
        connectionParams += "ssl=false"

        val badConnection = TestHelper.TEST_CONNECTION_CONFIG.copy(connectionParams = connectionParams)
        val result        = Await.result(RedshiftConnector.create(badConnection), timeout)
        result.left.value should beDatabaseErrorEqualWithoutCause(
          DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SSLError, "SSL is disabled")
        )
      }

      "connect ok" in {

        val connectorEither =
          Await.result(RedshiftConnector.create(TestHelper.TEST_CONNECTION_CONFIG), timeout)

        connectorEither shouldBe a[Right[_, _]]
      }
    }

    "#testConnection" should {

      "return ok in happy case" in {
        val connection =
          Await.result(RedshiftConnector.create(TestHelper.TEST_CONNECTION_CONFIG), timeout).toOption.get
        val result = Await.result(connection.testConnection(), timeout)
        result shouldBe Right({})
        connection.close()
      }

      "return error if cant connect" in {
        val badConnection = TestHelper.TEST_CONNECTION_CONFIG.copy(host = "asd.asd.asd")
        val connection    = Await.result(RedshiftConnector.create(badConnection), timeout)
        connection shouldBe a[Left[_, _]]
        connection.left.get should haveErrorCategoryAndErrorName(ErrorCategory.Timeout, ErrorName.ConnectionTimeout)
      }

    }

    trait QueryRunnerScope {
      lazy val connectionConfig = TestHelper.TEST_CONNECTION_CONFIG
      lazy val queryTimeout     = 20.second

      def runQuery(q: String): ConnectorResponse[Unit] =
        for {
          Right(connector) <- RedshiftConnector.create(connectionConfig)
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

    "custom error handling" should {
      "recognize syntax errors" in new QueryRunnerScope {
        val result = Await.result(runQuery("select from table"), timeout)

        result shouldBe a[Left[_, _]]
        result.left.get should haveErrorCategoryAndErrorName(
          ErrorCategory.FatalQueryExecution,
          ErrorName.SqlSyntaxError
        )
      }

      "recognize if a table is not found" in new QueryRunnerScope {
        val result = Await.result(runQuery("select * from a_non_existing_table"), timeout)

        result shouldBe a[Left[_, _]]
        result.left.get should haveErrorCategoryAndErrorName(
          ErrorCategory.FatalQueryExecution,
          ErrorName.TableNotFound
        )
      }
    }
  }
}
