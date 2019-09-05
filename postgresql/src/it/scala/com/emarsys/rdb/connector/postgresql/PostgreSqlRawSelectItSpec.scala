package com.emarsys.rdb.connector.postgresql

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName}
import com.emarsys.rdb.connector.postgresql.utils.SelectDbInitHelper
import com.emarsys.rdb.connector.test.CustomMatchers.beDatabaseErrorEqualWithoutCause
import com.emarsys.rdb.connector.test._
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

class PostgreSqlRawSelectItSpec
    extends TestKit(ActorSystem("PostgreSqlRawSelectItSpec"))
    with RawSelectItSpec
    with SelectDbInitHelper
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  implicit val materializer: Materializer = ActorMaterializer()

  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  override val awaitTimeout = 15.seconds

  override def afterAll(): Unit = {
    shutdown()
    cleanUpDb()
    connector.close()
  }

  override def beforeAll(): Unit = {
    initDb()
  }

  override val simpleSelect            = s"""SELECT * FROM "$aTableName";"""
  override val badSimpleSelect         = s"""SELECT * ForM "$aTableName""""
  override val simpleSelectNoSemicolon = s"""SELECT * FROM "$aTableName""""

  "#analyzeRawSelect" should {
    "return result" in {
      val result = getConnectorResult(connector.analyzeRawSelect(simpleSelect), awaitTimeout)

      result shouldEqual Seq(
        Seq("QUERY PLAN"),
        Seq(s"""Seq Scan on $aTableName  (cost=0.00..1.07 rows=7 width=521)""")
      )
    }
  }
  "#rawSelect" should {
    "return QueryTimeout when query takes more time than the timeout" in {
      val message  = "ERROR: canceling statement due to user request"
      val expected = DatabaseError(ErrorCategory.Timeout, ErrorName.QueryTimeout, message, None, None)
      val result   = connector.rawSelect("SELECT PG_SLEEP(6)", None, 5.second)
      val error    = the[DatabaseError] thrownBy getConnectorResult(result, awaitTimeout)

      error should beDatabaseErrorEqualWithoutCause(expected)
    }
  }

  "#projectedRawSelect" should {
    "return QueryTimeout when query takes more time than the timeout" in {
      val message  = "ERROR: canceling statement due to user request"
      val expected = DatabaseError(ErrorCategory.Timeout, ErrorName.QueryTimeout, message, None, None)
      val result   = connector.projectedRawSelect("SELECT PG_SLEEP(6) as sleep", Seq("sleep"), None, 5.second)
      val error    = the[DatabaseError] thrownBy getConnectorResult(result, awaitTimeout)

      error should beDatabaseErrorEqualWithoutCause(expected)
    }
  }

}
