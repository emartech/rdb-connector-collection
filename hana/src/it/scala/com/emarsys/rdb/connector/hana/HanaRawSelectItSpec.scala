package com.emarsys.rdb.connector.hana

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.emarsys.rdb.connector.hana.utils.SelectDbInitHelper
import com.emarsys.rdb.connector.test.RawSelectItSpec
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import com.emarsys.rdb.connector.test._

import scala.concurrent.{Await, ExecutionContextExecutor}

class HanaRawSelectItSpec
    extends TestKit(ActorSystem("HanaRawSelectItSpec"))
    with RawSelectItSpec
    with SelectDbInitHelper
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    shutdown()
    cleanUpDb()
    connector.close()
  }

  override def beforeAll(): Unit = {
    initDb()
  }

  implicit override val executionContext: ExecutionContextExecutor = system.dispatcher

  override val simpleSelect            = s"""SELECT * FROM "$aTableName";"""
  override val badSimpleSelect         = s"""SELECT * ForM "$aTableName""""
  override val simpleSelectNoSemicolon = s"""SELECT * FROM "$aTableName""""

  override val booleanValue0 = "false"
  override val booleanValue1 = "true"

  "#analyzeRawSelect" should {
    "return result" in {
      val result = getConnectorResult(connector.analyzeRawSelect(simpleSelect), awaitTimeout)

      val expectedResponse = Seq(
        Seq(
          "OPERATOR_NAME",
          "OPERATOR_DETAILS",
          "OPERATOR_PROPERTIES",
          "EXECUTION_ENGINE",
          "DATABASE_NAME",
          "SCHEMA_NAME",
          "TABLE_NAME",
          "TABLE_TYPE",
          "TABLE_SIZE",
          "OUTPUT_SIZE",
          "SUBTREE_COST",
          "OPERATOR_ID",
          "PARENT_OPERATOR_ID",
          "LEVEL",
          "POSITION"
        ),
        Seq(
          "PROJECT",
          s"$aTableName.A1, $aTableName.A2, $aTableName.A3",
          "",
          "HEX",
          "H00",
          null,
          null,
          null,
          null,
          "7.0",
          "2.1E-8",
          "1",
          null,
          "1",
          "1"
        ),
        Seq(
          "  COLUMN TABLE",
          "",
          "",
          "HEX",
          "",
          "IT_TEST",
          s"$aTableName",
          "COLUMN TABLE",
          "7.0",
          "7.0",
          "2.1E-8",
          "2",
          "1",
          "2",
          "1"
        )
      )

      result should equal(expectedResponse)
    }

    "cleanup explain_plan_table after analyze/validate queries" in {
      getConnectorResult(connector.analyzeRawSelect(simpleSelect), awaitTimeout)
      Await.result(connector.validateRawSelect(simpleSelect), awaitTimeout)
      Await.result(connector.validateProjectedRawSelect(simpleSelect, Seq("A1")), awaitTimeout)

      val result = getConnectorResult(connector.rawSelect("SELECT * FROM explain_plan_table", None, awaitTimeout), awaitTimeout)

      result should equal(Seq.empty)
    }

  }

}
