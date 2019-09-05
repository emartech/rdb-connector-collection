package com.emarsys.rdb.connector.postgresql

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors._
import com.emarsys.rdb.connector.postgresql.PostgreSqlConnector.PostgreSqlConnectionConfig
import com.emarsys.rdb.connector.postgresql.utils.TestHelper.TEST_CONNECTION_CONFIG
import com.emarsys.rdb.connector.test.CustomMatchers.beDatabaseErrorEqualWithoutCause
import org.scalatest.{AsyncWordSpecLike, BeforeAndAfterAll, EitherValues, Matchers}

import scala.concurrent.duration._

class PostgreSqlConnectorItSpec
    extends TestKit(ActorSystem("PostgreSqlConnectorItSpec"))
    with AsyncWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with EitherValues {

  implicit val mat = ActorMaterializer()

  private val timeoutMessage = "Connection is not available, request timed out after"

  override def afterAll: Unit = {
    shutdown()
  }

  "PostgreSqlConnector" when {

    val defaultConnection = TEST_CONNECTION_CONFIG

    "create connector" should {

      "connect success" in {
        PostgreSqlConnector.create(defaultConnection).map { result =>
          result.right.value
          succeed
        }
      }

      "connect fail when ssl disabled" in {
        val expected =
          DatabaseError(ErrorCategory.Internal, ErrorName.ConnectionConfigError, "SSL Error", None, None)
        val conn = defaultConnection.copy(connectionParams = "ssl=false")

        PostgreSqlConnector.create(conn).map { result =>
          result.left.value should beDatabaseErrorEqualWithoutCause(expected)
        }
      }

      "connect fail when sslrootcert modified" in {
        val expected =
          DatabaseError(ErrorCategory.Internal, ErrorName.ConnectionConfigError, "SSL Error", None, None)
        val conn = defaultConnection.copy(connectionParams = "sslrootcert=/root.crt")

        PostgreSqlConnector.create(conn).map { result =>
          result.left.value should beDatabaseErrorEqualWithoutCause(expected)
        }
      }

      "connect fail when sslmode modified" in {
        val expected =
          DatabaseError(ErrorCategory.Internal, ErrorName.ConnectionConfigError, "SSL Error", None, None)
        val conn = defaultConnection.copy(connectionParams = "sslmode=disable")

        PostgreSqlConnector.create(conn).map { result =>
          result.left.value should beDatabaseErrorEqualWithoutCause(expected)
        }
      }

      "connect fail when wrong certificate" in {
        val expected =
          DatabaseError(ErrorCategory.Timeout, ErrorName.ConnectionTimeout, timeoutMessage, None, None)
        val conn = defaultConnection.copy(certificate = "")

        PostgreSqlConnector.create(conn).map { result =>
          result.left.value should beDatabaseErrorEqualWithoutCause(expected)
        }
      }

      "connect fail when wrong host" in {
        val expected =
          DatabaseError(ErrorCategory.Timeout, ErrorName.ConnectionTimeout, timeoutMessage, None, None)
        val conn = defaultConnection.copy(host = "wrong")

        PostgreSqlConnector.create(conn).map { result =>
          result.left.value should beDatabaseErrorEqualWithoutCause(expected)
        }
      }

      "connect fail when wrong user" in {
        val expected =
          DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.AccessDeniedError, timeoutMessage, None, None)
        val conn = defaultConnection.copy(dbUser = "")

        PostgreSqlConnector.create(conn).map { result =>
          result.left.value should beDatabaseErrorEqualWithoutCause(expected)
        }
      }

      "connect fail when wrong password" in {
        val expected =
          DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.AccessDeniedError, timeoutMessage, None, None)
        val conn = defaultConnection.copy(dbPassword = "")

        PostgreSqlConnector.create(conn).map { result =>
          result.left.value should beDatabaseErrorEqualWithoutCause(expected)
        }
      }

    }

    "#testConnection" should {

      "success" in {
        for {
          result <- PostgreSqlConnector.create(defaultConnection)
          connector = result.right.value
          _ <- connector.testConnection()
          _ <- connector.close()
        } yield succeed
      }

    }

    def runSelect(q: String, connConfig: PostgreSqlConnectionConfig = defaultConnection): ConnectorResponse[Unit] =
      for {
        Right(connector) <- PostgreSqlConnector.create(connConfig)
        Right(source)    <- connector.rawSelect(q, limit = None, 5.second)
        res              <- sinkOrLeft(source)
        _                <- connector.close()
      } yield res

    def sinkOrLeft[T](source: Source[T, NotUsed]): ConnectorResponse[Unit] =
      source
        .runWith(Sink.ignore)
        .map[Either[ConnectorError, Unit]](_ => Right(()))
        .recover {
          case e: ConnectorError => Left[ConnectorError, Unit](e)
        }

    "custom error handling" should {
      "recognize syntax errors" in {
        val message = """ERROR: syntax error at or near "table""""
        val expectedError =
          DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SqlSyntaxError, message, None, None)

        runSelect("select from table").map { result =>
          result.left.value should beDatabaseErrorEqualWithoutCause(expectedError)
        }
      }

      "recognize access denied errors" in {
        val message = "ERROR: permission denied for relation access_denied"
        val expectedError =
          DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.AccessDeniedError, message, None, None)
        val connectionConfig = defaultConnection.copy(dbUser = "unprivileged", dbPassword = "unprivilegedpw")

        runSelect("select * from access_denied", connectionConfig).map { result =>
          result.left.value should beDatabaseErrorEqualWithoutCause(expectedError)
        }
      }

      "recognize query timeouts" in {
        val message       = "ERROR: canceling statement due to user request"
        val expectedError = DatabaseError(ErrorCategory.Timeout, ErrorName.QueryTimeout, message, None, None)

        runSelect("select pg_sleep(6)").map { result =>
          result.left.value should beDatabaseErrorEqualWithoutCause(expectedError)
        }
      }

      "recognize if a table is not found" in {
        val tableMissingMessage = """ERROR: relation "a_non_existing_table" does not exist"""
        val expectedError =
          DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.TableNotFound, tableMissingMessage, None, None)

        runSelect("select * from a_non_existing_table").map { result =>
          result.left.value should beDatabaseErrorEqualWithoutCause(expectedError)
        }
      }

      "recognize not found columns as syntax error" when {
        "column does not exists" in {
          val message = """ERROR: column "no_such_column" does not exist"""
          val expectedError =
            DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SqlSyntaxError, message, None, None)

          runSelect("select no_such_column from test").map { result =>
            result.left.value should beDatabaseErrorEqualWithoutCause(expectedError)
          }
        }

        "column does not exist on joined table" in {
          val message = "ERROR: column t2.id does not exist"
          val expectedError =
            DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SqlSyntaxError, message, None, None)

          runSelect("select t2.id from test t1 join test2 t2 on t1.id = t2.test_id").map { result =>
            result.left.value should beDatabaseErrorEqualWithoutCause(expectedError)
          }
        }
      }
    }
  }
}
