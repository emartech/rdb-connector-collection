package com.emarsys.rdb.connector.common.models

import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper.StringValue
import com.emarsys.rdb.connector.common.models.DataManipulation.UpdateDefinition
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName}

import com.emarsys.rdb.connector.common.models.SimpleSelect.{AllField, TableName}

import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

class ConnectorNotImplementedOperationSpec extends WordSpecLike with Matchers {

  implicit val executionCtx: ExecutionContext = ExecutionContext.global

  val tableName = "tableName"
  val sql       = "SQL"

  val defaultTimeout = 3.seconds
  val sqlTimeout     = 3.seconds

  def notImplementedError(message: String) =
    Left(
      DatabaseError(
        ErrorCategory.FatalQueryExecution,
        ErrorName.NotImplementedOperation,
        message
      )
    )

  class ConnectorTestScope extends Connector {
    override implicit val executionContext: ExecutionContext = executionCtx

    override def close() = ???
  }

  "default response NotImplementedOperation" should {
    "#testConnection" in new ConnectorTestScope {
      Await.result(testConnection(), defaultTimeout) shouldEqual notImplementedError("testConnection not implemented")
    }

    "#listTables" in new ConnectorTestScope {
      Await.result(listTables(), defaultTimeout) shouldEqual notImplementedError("listTables not implemented")
    }

    "#listTablesWithFields" in new ConnectorTestScope {
      Await.result(listTablesWithFields(), defaultTimeout) shouldEqual notImplementedError(
        "listTablesWithFields not implemented"
      )
    }

    "#listFields" in new ConnectorTestScope {
      Await.result(listFields(tableName), defaultTimeout) shouldEqual notImplementedError("listFields not implemented")
    }

    "#isOptimized" in new ConnectorTestScope {
      Await.result(isOptimized(tableName, Seq()), defaultTimeout) shouldEqual notImplementedError(
        "isOptimized not implemented"
      )
    }

    "#simpleSelect" in new ConnectorTestScope {
      Await.result(simpleSelect(SimpleSelect(AllField, TableName(tableName)), sqlTimeout), defaultTimeout) shouldEqual notImplementedError(
        "simpleSelect not implemented"
      )
    }

    "#rawSelect" in new ConnectorTestScope {
      Await.result(rawSelect(sql, None, sqlTimeout), defaultTimeout) shouldEqual notImplementedError(
        "rawSelect not implemented"
      )
    }

    "#validateRawSelect" in new ConnectorTestScope {
      Await.result(validateRawSelect(sql), defaultTimeout) shouldEqual notImplementedError(
        "validateRawSelect not implemented"
      )
    }

    "#analyzeRawSelect" in new ConnectorTestScope {
      Await.result(analyzeRawSelect(sql), defaultTimeout) shouldEqual notImplementedError(
        "analyzeRawSelect not implemented"
      )
    }

    "#projectedRawSelect" in new ConnectorTestScope {
      Await.result(projectedRawSelect(sql, Seq(), None, sqlTimeout), defaultTimeout) shouldEqual notImplementedError(
        "projectedRawSelect not implemented"
      )
    }

    "#validateProjectedRawSelect" in new ConnectorTestScope {
      Await.result(validateProjectedRawSelect(sql, Seq()), defaultTimeout) shouldEqual notImplementedError(
        "validateProjectedRawSelect not implemented"
      )
    }

    "#rawQuery" in new ConnectorTestScope {
      Await.result(rawQuery(sql, sqlTimeout), defaultTimeout) shouldEqual notImplementedError(
        "rawQuery not implemented"
      )
    }

    "#rawUpdate" in new ConnectorTestScope {
      val definitions = Seq(UpdateDefinition(Map("a" -> StringValue("1")), Map("b" -> StringValue("2"))))
      Await.result(rawUpdate(tableName, definitions), defaultTimeout) shouldBe notImplementedError(
        "rawUpdate not implemented"
      )
    }

    "#rawInsertData" in new ConnectorTestScope {
      val records = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      Await.result(rawInsertData(tableName, records), defaultTimeout) shouldBe notImplementedError(
        "rawInsertData not implemented"
      )

    }

    "#rawReplaceData" in new ConnectorTestScope {
      val records = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      Await.result(rawReplaceData(tableName, records), defaultTimeout) shouldBe notImplementedError(
        "rawReplaceData not implemented"
      )
    }

    "#rawUpsert" in new ConnectorTestScope {
      val records = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))

      Await.result(rawUpsert(tableName, records), defaultTimeout) shouldBe notImplementedError(
        "rawUpsert not implemented"
      )
    }

    "#rawDelete" in new ConnectorTestScope {
      val criteria = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))

      Await.result(rawDelete(tableName, criteria), defaultTimeout) shouldBe notImplementedError(
        "rawDelete not implemented"
      )
    }
  }
}
