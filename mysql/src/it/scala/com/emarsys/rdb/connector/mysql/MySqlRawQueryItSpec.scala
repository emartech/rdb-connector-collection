package com.emarsys.rdb.connector.mysql

import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName}
import com.emarsys.rdb.connector.common.models.SimpleSelect
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import com.emarsys.rdb.connector.mysql.utils.SelectDbInitHelper
import com.emarsys.rdb.connector.test.CustomMatchers.beDatabaseErrorEqualWithoutCause
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, EitherValues}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpecLike

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class MySqlRawQueryItSpec
    extends TestKit(ActorSystem("MySqlRawQueryItSpec"))
    with SelectDbInitHelper
    with AsyncWordSpecLike
    with Matchers
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with EitherValues {

  implicit val exco: ExecutionContext = ec

  val uuid = UUID.randomUUID().toString.replace("-", "")

  val aTableName: String = s"raw_query_tables_table_$uuid"
  val bTableName: String = s"temp_$uuid"

  val awaitTimeout = 10.seconds
  val queryTimeout = 10.seconds

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
        val message =
          "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'invalid sql' at line 1"
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
        val timeoutMessage = "Statement cancelled due to timeout or client request"
        val expectedError =
          DatabaseError(ErrorCategory.Timeout, ErrorName.QueryTimeout, timeoutMessage, None, None)
        val query = s"DELETE FROM $aTableName WHERE A1 = SLEEP(12)"

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
