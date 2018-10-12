package com.emarsys.rbd.connector.bigquery

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import com.emarsys.rbd.connector.bigquery.utils.TestHelper
import com.emarsys.rdb.connector.bigquery.BigQueryConnector
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors._
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._

class BigQueryConnectorItSpec extends TestKit(ActorSystem()) with WordSpecLike with Matchers with BeforeAndAfterAll {

  implicit val mat      = ActorMaterializer()
  override def afterAll = shutdown()

  val testConnection = TestHelper.TEST_CONNECTION_CONFIG

  "BigQueryConnector" when {

    "#testConnection" should {

      "return ok in happy case" in {
        val connection = Await.result(BigQueryConnector(testConnection)(system), 3.seconds).toOption.get
        val result     = Await.result(connection.testConnection(), 5.seconds)
        result shouldBe Right(())
        connection.close()
      }

      "return error if invalid project id" in {
        val badConnection = testConnection.copy(projectId = "asd")
        val connection    = Await.result(BigQueryConnector(badConnection)(system), 3.seconds).toOption.get
        val result        = Await.result(connection.testConnection(), 5.seconds)
        result should matchPattern { case Left(ConnectionError(_)) => }
      }

      "return error if invalid dataset" in {
        val badConnection = testConnection.copy(dataset = "asd")
        val connection    = Await.result(BigQueryConnector(badConnection)(system), 3.seconds).toOption.get
        val result        = Await.result(connection.testConnection(), 5.seconds)
        result should matchPattern { case Left(ConnectionError(_)) => }
      }
    }

    "custom error handling" should {
      "recognize syntax errors" in new QueryRunnerScope {
        val result = Await.result(runQuery("select from test.table"), 3.seconds)

        result shouldBe a[Left[_, _]]
        result.left.get shouldBe a[SqlSyntaxError]
      }

      "recognize not found tables" in new QueryRunnerScope {
        val result = Await.result(runQuery("select * from test.a_non_existing_table"), 3.seconds)

        result shouldBe a[Left[_, _]]
        result.left.get shouldBe a[TableNotFound]
      }

      "recognize query timeouts" in new QueryRunnerScope {
        override lazy val queryTimeout = 100.millis
        val result                     = Await.result(runQuery("select * from test.test_table"), 1.second)

        result shouldBe a[Left[_, _]]
        result.left.get shouldBe a[QueryTimeout]
      }
    }

    trait QueryRunnerScope {
      lazy val queryTimeout = 3.second
      implicit val ec       = system.dispatcher

      def runQuery(q: String): ConnectorResponse[Unit] =
        for {
          Right(connector) <- BigQueryConnector(testConnection)(system)
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
  }
}
