package com.emarsys.rbd.connector.bigquery

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import com.emarsys.rbd.connector.bigquery.utils.TestHelper
import com.emarsys.rdb.connector.bigquery.BigQueryConnector
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName}
import com.emarsys.rdb.connector.test.CustomMatchers.haveErrorCategoryAndErrorName
import org.scalatest.{AsyncWordSpecLike, BeforeAndAfterAll, EitherValues, Matchers}

import scala.concurrent.duration._

class BigQueryConnectorItSpec
    extends TestKit(ActorSystem("BigQueryConnectorItSpec"))
    with AsyncWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with EitherValues {

  implicit val mat      = ActorMaterializer()
  override def afterAll = shutdown()

  val testConnection = TestHelper.TEST_CONNECTION_CONFIG

  "BigQueryConnector" when {

    "#testConnection" should {

      "return ok in happy case" in {
        for {
          connector <- BigQueryConnector(testConnection)(system)
          _         <- connector.right.value.testConnection()
        } yield succeed
      }

      "return error if invalid project id" in {
        val badConnection = testConnection.copy(projectId = "asd")

        for {
          connector <- BigQueryConnector(badConnection)(system)
          error     <- connector.right.value.testConnection()
        } yield {
          error.left.value should haveErrorCategoryAndErrorName(
            ErrorCategory.FatalQueryExecution,
            ErrorName.TableNotFound
          )
        }
      }

      "return error if invalid dataset" in {
        val badConnection = testConnection.copy(dataset = "asd")

        for {
          connector <- BigQueryConnector(badConnection)(system)
          error     <- connector.right.value.testConnection()
        } yield {
          error.left.value should haveErrorCategoryAndErrorName(
            ErrorCategory.FatalQueryExecution,
            ErrorName.TableNotFound
          )
        }
      }
    }

    "custom error handling" should {
      "recognize syntax errors" in {
        rawSelect("select from test.table").map(error =>
          error.left.value should haveErrorCategoryAndErrorName(
            ErrorCategory.FatalQueryExecution,
            ErrorName.SqlSyntaxError
          )
        )
      }

      "recognize not found tables" in {
        rawSelect("select * from test.a_non_existing_table").map(error =>
          error.left.value should haveErrorCategoryAndErrorName(
            ErrorCategory.FatalQueryExecution,
            ErrorName.TableNotFound
          )
        )
      }

      "recognize query timeouts" in {
        rawSelect("select * from test.test_table", timeout = 100.millis).map(error =>
          error.left.value should haveErrorCategoryAndErrorName(
            ErrorCategory.Timeout,
            ErrorName.QueryTimeout
          )
        )
      }
    }

    def rawSelect(q: String, timeout: FiniteDuration = 3.seconds): ConnectorResponse[Unit] =
      for {
        connector <- BigQueryConnector(testConnection)(system)
        source    <- connector.right.value.rawSelect(q, limit = None, timeout)
        res       <- sinkOrLeft(source.right.value)
      } yield res

    def sinkOrLeft[T](source: Source[T, NotUsed]): ConnectorResponse[Unit] =
      source
        .runWith(Sink.ignore)
        .map[Either[DatabaseError, Unit]](_ => Right(()))
        .recover {
          case e: DatabaseError => Left[DatabaseError, Unit](e)
        }
  }
}
