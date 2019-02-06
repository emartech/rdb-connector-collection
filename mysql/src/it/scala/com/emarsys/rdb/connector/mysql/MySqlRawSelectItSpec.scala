package com.emarsys.rdb.connector.mysql

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.models.Errors.{QueryTimeout, SqlSyntaxError}
import com.emarsys.rdb.connector.mysql.utils.SelectDbInitHelper
import com.emarsys.rdb.connector.test._
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

class MySqlRawSelectItSpec
    extends TestKit(ActorSystem())
    with RawSelectItSpec
    with SelectDbInitHelper
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  implicit val materializer: Materializer = ActorMaterializer()

  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  override def afterAll(): Unit = {
    system.terminate()
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
        Seq("1", "SIMPLE", s"$aTableName", "index", null, s"${aTableName.dropRight(5)}_idx2", "7", null, "7", "Using index")
      )

      val mysql57And8Response = Seq(
        Seq("id", "select_type", "table", "partitions", "type", "possible_keys", "key", "key_len", "ref", "rows", "filtered", "Extra"),
        Seq("1", "SIMPLE", s"$aTableName", null, "index", null, s"${aTableName.dropRight(5)}_idx2", "7", null, "7", "100.0", "Using index")
      )

      result should (
        equal(mysql56Response) or
        equal(mysql57And8Response)
      )
    }
  }

  "#rawSelect" should {

    "return SqlSyntaxError when there is a syntax error in the query" in {
      val result = connector.rawSelect(missingColumnSelect, None, queryTimeout)

      a[SqlSyntaxError] should be thrownBy {
        getConnectorResult(result, awaitTimeout)
      }
    }

    "return SqlSyntaxError when update query given" in {
      val result = connector.rawSelect(s"UPDATE `$aTableName` SET key = '12' WHERE 1 = 2;", None, queryTimeout)

      a[SqlSyntaxError] should be thrownBy {
        getConnectorResult(result, awaitTimeout)
      }
    }

    "return QueryTimeout when query takes more time than the timeout" in {
      val result = connector.rawSelect("SELECT SLEEP(6)", None, 5.second)

      the[Exception] thrownBy getConnectorResult(result, awaitTimeout) shouldBe QueryTimeout("Statement cancelled due to timeout or client request")
    }

  }

  "#projectedRawSelect" should {

    "return QueryTimeout when query takes more time than the timeout" in {
      val result = connector.projectedRawSelect("SELECT SLEEP(10) as sleep", Seq("sleep"), None, 5.second)

      a[QueryTimeout] should be thrownBy {
        getConnectorResult(result, awaitTimeout)
      }
    }

  }

}
