package com.emarsys.rdb.connector.postgresql

import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName}
import com.emarsys.rdb.connector.common.models.SimpleSelect
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import com.emarsys.rdb.connector.postgresql.utils.SelectDbInitHelper
import com.emarsys.rdb.connector.test.CustomMatchers.beDatabaseErrorEqualWithoutCause
import com.emarsys.rdb.connector.test.util.EitherValues
import org.scalatest.{AsyncWordSpecLike, BeforeAndAfterAll, BeforeAndAfterEach, Matchers}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class PostgreSqlRawQueryItSpec
    extends TestKit(ActorSystem("PostgreSqlRawQueryItSpec"))
    with SelectDbInitHelper
    with AsyncWordSpecLike
    with Matchers
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with EitherValues {

  implicit val excon: ExecutionContext = ec

  val uuid = UUID.randomUUID().toString.replace("-", "")

  val aTableName: String = s"raw_query_tables_table_$uuid"
  val bTableName: String = s"temp_$uuid"

  val awaitTimeout = 10.seconds
  val queryTimeout = 5.seconds

  override def afterAll(): Unit = {
    shutdown()
    connector.close()
  }

  override def beforeEach(): Unit = {
    initDb()
  }

  override def afterEach(): Unit = {
    cleanUpDb()
  }

  s"RawQuerySpec $uuid" when {

    "#rawQuery" should {

      "validation error" in {
        val message    = """ERROR: syntax error at or near "invalid""""
        val expected   = DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SqlSyntaxError, message, None, None)
        val invalidSql = "invalid sql"

        connector.rawQuery(invalidSql, queryTimeout).map { result =>
          result.left.value should beDatabaseErrorEqualWithoutCause(expected)
        }
      }

      "run a delete query" in {
        for {
          _      <- connector.rawQuery(s"DELETE FROM $aTableName WHERE A1!='v1'", queryTimeout)
          result <- selectAll(aTableName)
        } yield result shouldEqual Vector(Vector("v1", "1", "1"))
      }

      "return SqlSyntaxError when select query given" in {
        val message  = "Update statements should not return a ResultSet"
        val expected = DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SqlSyntaxError, message, None, None)

        connector.rawQuery(s"SELECT 1;", queryTimeout).map { result =>
          result.left.value should beDatabaseErrorEqualWithoutCause(expected)
        }
      }

      "return QueryTimeout when query takes more time than the timeout" in {
        val query          = s"DELETE FROM $aTableName WHERE A1 = pg_sleep(12)::text"
        val timeoutMessage = "ERROR: canceling statement due to user request"
        val expectedError =
          DatabaseError(ErrorCategory.Timeout, ErrorName.QueryTimeout, timeoutMessage, None, None)

        connector.rawQuery(query, 1.second).map { result =>
          result.left.value should beDatabaseErrorEqualWithoutCause(expectedError)
        }
      }

    }
  }

  private def selectAll(tableName: String) = {
    connector
      .simpleSelect(SimpleSelect(AllField, TableName(tableName)), queryTimeout)
      .flatMap(result => result.value.runWith(Sink.seq))
      .map(_.drop(1))
  }
}
