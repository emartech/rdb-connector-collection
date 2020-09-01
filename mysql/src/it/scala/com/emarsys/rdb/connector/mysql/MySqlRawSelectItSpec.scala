package com.emarsys.rdb.connector.mysql

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.models.Errors.ErrorCategory.FatalQueryExecution
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName}
import com.emarsys.rdb.connector.mysql.utils.SelectDbInitHelper
import com.emarsys.rdb.connector.test.CustomMatchers.beDatabaseErrorEqualWithoutCause
import com.emarsys.rdb.connector.test._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

class MySqlRawSelectItSpec
    extends TestKit(ActorSystem("MySqlRawSelectItSpec"))
    with RawSelectItSpec
    with SelectDbInitHelper
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val timeoutMessage = "Statement cancelled due to timeout or client request"

  override def afterAll(): Unit = {
    shutdown()
    cleanUpDb()
    connector.close()
  }

  override def beforeAll(): Unit = {
    initDb()
  }

  override val simpleSelect            = s"SELECT * FROM `$aTableName`;"
  override val badSimpleSelect         = s"SELECT * ForM `$aTableName`"
  override val simpleSelectNoSemicolon = s"""SELECT * FROM `$aTableName`"""
  val missingColumnSelect              = s"SELECT nope FROM $aTableName"

  "#analyzeRawSelect" should {
    "return result" in {
      val result = getConnectorResult(connector.analyzeRawSelect(simpleSelect), awaitTimeout)

      val mysql56Response = Seq(
        Seq("id", "select_type", "table", "type", "possible_keys", "key", "key_len", "ref", "rows", "Extra"),
        Seq(
          "1",
          "SIMPLE",
          s"$aTableName",
          "index",
          null,
          s"${aTableName.dropRight(5)}_idx2",
          "7",
          null,
          "7",
          "Using index"
        )
      )

      val mysql57And8Response = Seq(
        Seq(
          "id",
          "select_type",
          "table",
          "partitions",
          "type",
          "possible_keys",
          "key",
          "key_len",
          "ref",
          "rows",
          "filtered",
          "Extra"
        ),
        Seq(
          "1",
          "SIMPLE",
          s"$aTableName",
          null,
          "index",
          null,
          s"${aTableName.dropRight(5)}_idx2",
          "7",
          null,
          "7",
          "100.0",
          "Using index"
        )
      )

      result should (
        equal(mysql56Response) or
          equal(mysql57And8Response)
      )
    }
  }

  "#rawSelect" should {

    "return SqlSyntaxError when there is a syntax error in the query" in {
      val message  = "Unknown column 'nope' in 'field list'"
      val expected = DatabaseError(FatalQueryExecution, ErrorName.SqlSyntaxError, message, None, None)
      val error = the[DatabaseError] thrownBy getConnectorResult(
        connector.rawSelect(missingColumnSelect, None, queryTimeout),
        awaitTimeout
      )

      error should beDatabaseErrorEqualWithoutCause(expected)
    }

    "return SqlSyntaxError when update query given" in {
      val message =
        "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'key = '12' WHERE 1 = 2' at line 1"
      val expected = DatabaseError(FatalQueryExecution, ErrorName.SqlSyntaxError, message, None, None)
      val error = the[DatabaseError] thrownBy getConnectorResult(
        connector.rawSelect(s"UPDATE `$aTableName` SET key = '12' WHERE 1 = 2;", None, queryTimeout),
        awaitTimeout
      )

      error should beDatabaseErrorEqualWithoutCause(expected)
    }

    "return QueryTimeout when query takes more time than the timeout" in {
      val expected = DatabaseError(ErrorCategory.Timeout, ErrorName.QueryTimeout, timeoutMessage, None, None)
      val error = the[DatabaseError] thrownBy getConnectorResult(
        connector.rawSelect("SELECT SLEEP(6)", None, 5.second),
        awaitTimeout
      )

      error should beDatabaseErrorEqualWithoutCause(expected)
    }

  }

  "#projectedRawSelect" should {

    "return QueryTimeout when query takes more time than the timeout" in {
      val expected = DatabaseError(ErrorCategory.Timeout, ErrorName.QueryTimeout, timeoutMessage, None, None)
      val error = the[DatabaseError] thrownBy getConnectorResult(
        connector.projectedRawSelect("SELECT SLEEP(10) as sleep", Seq("sleep"), None, 5.second),
        awaitTimeout
      )

      error should beDatabaseErrorEqualWithoutCause(expected)
    }
  }
}
