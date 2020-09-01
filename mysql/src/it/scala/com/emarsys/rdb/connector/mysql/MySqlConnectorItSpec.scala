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
import com.emarsys.rdb.connector.test.CustomMatchers.beDatabaseErrorEqualWithoutCause
import com.emarsys.rdb.connector.test.util.EitherValues
import org.scalatest.{AsyncWordSpecLike, BeforeAndAfterAll, Matchers}

import scala.concurrent.duration._

class MySqlConnectorItSpec
    extends TestKit(ActorSystem("mysql-connector-it-test"))
    with AsyncWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with EitherValues {

  implicit val mat: ActorMaterializer = ActorMaterializer()
  override def afterAll(): Unit = {
    shutdown()
  }

  val timeoutMessage = "Connection is not available, request timed out after"

  "MySqlConnectorItSpec" when {

    val testConnectorConfig = TestHelper.TEST_CONNECTOR_CONFIG
    val testConnection      = TestHelper.TEST_CONNECTION_CONFIG

    "create connector" should {

      "connect success" in {
        withClue("We should have received back a connector") {
          MySqlConnector.create(testConnection, testConnectorConfig).map { connector =>
            connector.value.close()
            succeed
          }
        }
      }

      "connect fail when wrong certificate format" in {
        val conn = testConnection.copy(certificate = "")
        val expectedError =
          DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SSLError, "Wrong SSL cert format", None, None)

        MySqlConnector.create(conn, testConnectorConfig).map { result =>
          result.left.value should beDatabaseErrorEqualWithoutCause(expectedError)
        }
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
        val expectedError =
          DatabaseError(
            ErrorCategory.Timeout,
            ErrorName.ConnectionTimeout,
            "Connection is not available, request timed out after",
            None,
            None
          )

        MySqlConnector.create(conn, testConnectorConfig.copy(verifyServerCertificate = true)).map { result =>
          result.left.value should beDatabaseErrorEqualWithoutCause(expectedError)
        }
      }

      "connect fail when wrong host" in {
        val conn = testConnection.copy(host = "wrong")
        val expectedError =
          DatabaseError(ErrorCategory.Timeout, ErrorName.ConnectionTimeout, timeoutMessage, None, None)

        MySqlConnector.create(conn, testConnectorConfig).map { result =>
          result.left.value should beDatabaseErrorEqualWithoutCause(expectedError)
        }
      }

      "connect fail when wrong user" in {
        val conn = testConnection.copy(dbUser = "")
        val expectedError =
          DatabaseError(ErrorCategory.Timeout, ErrorName.ConnectionTimeout, timeoutMessage, None, None)

        MySqlConnector.create(conn, testConnectorConfig).map { result =>
          result.left.value should beDatabaseErrorEqualWithoutCause(expectedError)
        }
      }

      "connect fail when wrong password" in {
        val conn = testConnection.copy(dbPassword = "")
        val expectedError =
          DatabaseError(ErrorCategory.Timeout, ErrorName.ConnectionTimeout, timeoutMessage, None, None)

        MySqlConnector.create(conn, testConnectorConfig).map { result =>
          result.left.value should beDatabaseErrorEqualWithoutCause(expectedError)
        }
      }

    }

    "test connection" should {

      "return success" in {
        for {
          result <- MySqlConnector.create(testConnection, testConnectorConfig)
          connector = result.value
          _ <- connector.testConnection()
          _ <- connector.close()
        } yield succeed
      }

      "custom error handling" should {
        import cats.instances.future._
        def runSelect(q: String): ConnectorResponse[Unit] =
          (for {
            connector <- EitherT(MySqlConnector.create(testConnection, testConnectorConfig))
            source    <- EitherT(connector.rawSelect(q, limit = None, timeout = 1.second))
            res       <- EitherT(sinkOrLeft(source))
            _ = connector.close()
          } yield res).value

        def runQuery(q: String): ConnectorResponse[Int] =
          (for {
            connector <- EitherT(MySqlConnector.create(testConnection, testConnectorConfig))
            source    <- EitherT(connector.rawQuery(q, timeout = 1.second))
            _ = connector.close()
          } yield source).value

        def sinkOrLeft[T](source: Source[T, NotUsed]): ConnectorResponse[Unit] =
          source
            .runWith(Sink.ignore)
            .map[Either[DatabaseError, Unit]](_ => Right(()))
            .recover {
              case e: DatabaseError => Left[DatabaseError, Unit](e)
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
