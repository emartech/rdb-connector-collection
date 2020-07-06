package com.emarsys.rdb.connector.snowflake

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.models.Errors.DatabaseError
import com.emarsys.rdb.connector.common.models.Errors.ErrorCategory.Timeout
import com.emarsys.rdb.connector.common.models.Errors.ErrorName.QueryTimeout
import com.emarsys.rdb.connector.snowflake.utils.SelectDbInitHelper
import com.emarsys.rdb.connector.test.CustomMatchers.haveErrorCategoryAndErrorName
import com.emarsys.rdb.connector.test._
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

class SnowflakeRawSelectItSpec
  extends TestKit(ActorSystem("SnowflakeRawSelectItSpec"))
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

  override val simpleSelect = s"""SELECT * FROM "$aTableName";"""
  override val badSimpleSelect = s"""SELECT * ForM "$aTableName""""
  override val simpleSelectNoSemicolon = s"""SELECT * FROM "$aTableName""""

  override val booleanValue1 = "TRUE"
  override val booleanValue0 = "FALSE"

  "#analyzeRawSelect" should {
    "return result" in {
      val result = getConnectorResult(connector.analyzeRawSelect(simpleSelect), awaitTimeout)

      result shouldEqual Seq(
        Seq("step", "id", "parent", "operation", "objects", "alias", "expressions", "partitionsTotal", "partitionsAssigned", "bytesAssigned"),
        Seq(null, null, null, "GlobalStats", null, null, null, "1", "1", "1024"),
        Seq("1", "0", null, "Result", null, null, s""""$aTableName".A1, "$aTableName".A2, "$aTableName".A3""", null, null, null),
        Seq("1", "1", "0", "TableScan", s"""TEST_DB.EMS."$aTableName"""", null, "A1, A2, A3", "1", "1", "1024")
      )
    }
  }

  "#rawSelect" should {
    "return QueryTimeout when query takes more time than the timeout" in {
      val result = connector.rawSelect("CALL SYSTEM$WAIT(5)", None, 1.second)
      the[DatabaseError] thrownBy getConnectorResult(result, awaitTimeout) should haveErrorCategoryAndErrorName(Timeout, QueryTimeout)
    }
  }

  "#projectedRawSelect" should {
    "return QueryTimeout when query takes more time than the timeout" in {
      val result = connector.projectedRawSelect("""SELECT SYSTEM$WAIT(5) as "sleep"""", Seq("sleep"), None, 1.second)
      the[DatabaseError] thrownBy getConnectorResult(result, awaitTimeout) should haveErrorCategoryAndErrorName(Timeout, QueryTimeout)
    }
  }

}
