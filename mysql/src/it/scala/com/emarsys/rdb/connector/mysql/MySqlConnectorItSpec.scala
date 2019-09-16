package com.emarsys.rdb.connector.mysql

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import cats.data.EitherT
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors._
import com.emarsys.rdb.connector.mysql.MySqlConnector.MySqlConnectorConfig
import com.emarsys.rdb.connector.mysql.utils.TestHelper
import com.emarsys.rdb.connector.test.CustomMatchers.beDatabaseErrorEqualWithoutCause
import org.scalatest.{AsyncWordSpecLike, BeforeAndAfterAll, EitherValues, Matchers}

import scala.concurrent.duration._

class MySqlConnectorItSpec
    extends TestKit(ActorSystem("mysql-connector-it-test"))
    with AsyncWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with EitherValues {

  implicit val mat: ActorMaterializer = ActorMaterializer()
  override def afterAll: Unit = {
    shutdown()
  }

  val timeoutMessage = "Connection is not available, request timed out after"

  "MySqlConnectorItSpec" when {

    val testConnection = TestHelper.TEST_CONNECTION_CONFIG

    val config = MySqlConnectorConfig(configPath = "mysqldb", verifyServerCertificate = false)

    "create connector" should {

      "connect success" in {
        withClue("We should have received back a connector") {
          MySqlConnector.create(testConnection, config).map { connector =>
            connector.right.value.close()
            succeed
          }
        }
      }

      "connect fail when wrong certificate" in {
        val conn = testConnection.copy(certificate = "")
        val expectedError =
          DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SSLError, "Wrong SSL cert format", None, None)

        MySqlConnector.create(conn).map { result =>
          result.left.value should beDatabaseErrorEqualWithoutCause(expectedError)
        }
      }

      "connect fail when wrong host" in {
        val conn = testConnection.copy(host = "wrong")
        val expectedError =
          DatabaseError(ErrorCategory.Timeout, ErrorName.ConnectionTimeout, timeoutMessage, None, None)

        MySqlConnector.create(conn).map { result =>
          result.left.value should beDatabaseErrorEqualWithoutCause(expectedError)
        }
      }

      "connect fail when wrong user" in {
        val conn = testConnection.copy(dbUser = "")
        val expectedError =
          DatabaseError(ErrorCategory.Timeout, ErrorName.ConnectionTimeout, timeoutMessage, None, None)

        MySqlConnector.create(conn).map { result =>
          result.left.value should beDatabaseErrorEqualWithoutCause(expectedError)
        }
      }

      "connect fail when wrong password" in {
        val conn = testConnection.copy(dbPassword = "")
        val expectedError =
          DatabaseError(ErrorCategory.Timeout, ErrorName.ConnectionTimeout, timeoutMessage, None, None)

        MySqlConnector.create(conn).map { result =>
          result.left.value should beDatabaseErrorEqualWithoutCause(expectedError)
        }
      }

    }

    "test connection" should {

      "return success" in {
        for {
          result <- MySqlConnector.create(testConnection, config)
          connector = result.right.value
          _ <- connector.testConnection()
          _ <- connector.close()
        } yield succeed
      }

      "custom error handling" should {
        import cats.instances.future._
        def runSelect(q: String): ConnectorResponse[Unit] =
          (for {
            connector <- EitherT(MySqlConnector.create(testConnection, config))
            source    <- EitherT(connector.rawSelect(q, limit = None, timeout = 1.second))
            res       <- EitherT(sinkOrLeft(source))
            _ = connector.close()
          } yield res).value

        def runQuery(q: String): ConnectorResponse[Int] =
          (for {
            connector <- EitherT(MySqlConnector.create(testConnection, config))
            source    <- EitherT(connector.rawQuery(q, timeout = 1.second))
            _ = connector.close()
          } yield source).value

        def sinkOrLeft[T](source: Source[T, NotUsed]): ConnectorResponse[Unit] =
          source
            .runWith(Sink.ignore)
            .map[Either[ConnectorError, Unit]](_ => Right(()))
            .recover {
              case e: ConnectorError => Left[ConnectorError, Unit](e)
            }

        "recognize syntax errors" in {
          val message =
            "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'from table' at line 1"
          val expectedError =
            DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SqlSyntaxError, message, None, None)

          runSelect("select from table").map { result =>
            result.left.value should beDatabaseErrorEqualWithoutCause(expectedError)
          }
        }

        "recognize access denied errors" in {
          val message =
            "Access denied; you need (at least one of) the PROCESS privilege(s) for this operation"
          val expectedError =
            DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.AccessDeniedError, message, None, None)

          runSelect("select * from information_schema.innodb_metrics").map { result =>
            result.left.value should beDatabaseErrorEqualWithoutCause(expectedError)
          }
        }

        "return SqlSyntaxError when conflicting collation tables joined" in {
          val message =
            "Illegal mix of collations (utf8mb4_unicode_ci,IMPLICIT) and (utf8mb4_hungarian_ci,IMPLICIT) for operation '='"
          val expectedError =
            DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SqlSyntaxError, message, None, None)

          val createConflictingCollationTable1 =
            s"""CREATE TABLE c1 (
               |    c VARCHAR(255)
               |        COLLATE utf8mb4_unicode_ci
               |);""".stripMargin

          val createConflictingCollationTable2 =
            s"""CREATE TABLE c2 (
               |    c VARCHAR(255)
               |        COLLATE utf8mb4_hungarian_ci
               |);""".stripMargin

          for {
            _      <- runQuery(createConflictingCollationTable1)
            _      <- runQuery(createConflictingCollationTable2)
            result <- runSelect(s"SELECT * FROM c1 JOIN c2 ON (c1.c = c2.c)")
          } yield result.left.value should beDatabaseErrorEqualWithoutCause(expectedError)
        }
      }
    }
  }
}
