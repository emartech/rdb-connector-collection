package com.emarsys.rdb.connector.common.models

import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper.StringValue
import com.emarsys.rdb.connector.common.models.DataManipulation.UpdateDefinition
import com.emarsys.rdb.connector.common.models.Errors.NotImplementedOperation
import com.emarsys.rdb.connector.common.models.SimpleSelect.{AllField, TableName}
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

class ConnectorNotImplementedOperationSpec extends WordSpecLike with Matchers {

  implicit val executionCtx: ExecutionContext = ExecutionContext.global

  val tableName = "tableName"
  val sql = "SQL"

  val defaultTimeout = 3.seconds
  val sqlTimeout = 3.seconds

  class ConnectorTestScope extends Connector {
    override implicit val executionContext: ExecutionContext = executionCtx

    override def close() = ???
  }

  "default response NotImplementedOperation" should {
    "#testConnection" in new ConnectorTestScope {
      Await.result(testConnection(), defaultTimeout) shouldEqual Left(NotImplementedOperation("testConnection not implemented"))
    }
    "#listTables" in new ConnectorTestScope {
      Await.result(listTables(), defaultTimeout) shouldEqual Left(NotImplementedOperation("listTables not implemented"))
    }
    "#listTablesWithFields" in new ConnectorTestScope {
      Await.result(listTablesWithFields(), defaultTimeout) shouldEqual Left(NotImplementedOperation("listTablesWithFields not implemented"))
    }
    "#listFields" in new ConnectorTestScope {
      Await.result(listFields(tableName), defaultTimeout) shouldEqual Left(NotImplementedOperation("listFields not implemented"))
    }
    "#isOptimized" in new ConnectorTestScope {
      Await.result(isOptimized(tableName, Seq()), defaultTimeout) shouldEqual Left(NotImplementedOperation("isOptimized not implemented"))
    }
    "#simpleSelect" in new ConnectorTestScope {
      Await.result(simpleSelect(SimpleSelect(AllField, TableName(tableName)), sqlTimeout), defaultTimeout) shouldEqual Left(NotImplementedOperation("simpleSelect not implemented"))
    }
    "#rawSelect" in new ConnectorTestScope {
      Await.result(rawSelect(sql, None, sqlTimeout), defaultTimeout) shouldEqual Left(NotImplementedOperation("rawSelect not implemented"))
    }
    "#validateRawSelect" in new ConnectorTestScope {
      Await.result(validateRawSelect(sql), defaultTimeout) shouldEqual Left(NotImplementedOperation("validateRawSelect not implemented"))
    }
    "#analyzeRawSelect" in new ConnectorTestScope {
      Await.result(analyzeRawSelect(sql), defaultTimeout) shouldEqual Left(NotImplementedOperation("analyzeRawSelect not implemented"))
    }
    "#projectedRawSelect" in new ConnectorTestScope {
      Await.result(projectedRawSelect(sql, Seq(), None, sqlTimeout), defaultTimeout) shouldEqual Left(NotImplementedOperation("projectedRawSelect not implemented"))
    }
    "#validateProjectedRawSelect" in new ConnectorTestScope {
      Await.result(validateProjectedRawSelect(sql, Seq()), defaultTimeout) shouldEqual Left(NotImplementedOperation("validateProjectedRawSelect not implemented"))
    }
    "#rawQuery" in new ConnectorTestScope {
      Await.result(rawQuery(sql, sqlTimeout), defaultTimeout) shouldEqual Left(NotImplementedOperation("rawQuery not implemented"))
    }
    "#rawUpdate" in new ConnectorTestScope {
      val definitions = Seq(UpdateDefinition(Map("a" -> StringValue("1")), Map("b" -> StringValue("2"))))
      Await.result(rawUpdate(tableName, definitions), defaultTimeout) shouldBe Left(NotImplementedOperation("rawUpdate not implemented"))
    }
    "#rawInsertData" in new ConnectorTestScope {
      val records = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      Await.result(rawInsertData(tableName, records), defaultTimeout) shouldBe Left(NotImplementedOperation("rawInsertData not implemented"))
    }
    "#rawReplaceData" in new ConnectorTestScope {
      val records = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      Await.result(rawReplaceData(tableName, records), defaultTimeout) shouldBe Left(NotImplementedOperation("rawReplaceData not implemented"))
    }
    "#rawUpsert" in new ConnectorTestScope {
      val records = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      Await.result(rawUpsert(tableName, records), defaultTimeout) shouldBe Left(NotImplementedOperation("rawUpsert not implemented"))
    }
    "#rawDelete" in new ConnectorTestScope {
      val criteria = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      Await.result(rawDelete(tableName, criteria), defaultTimeout) shouldBe Left(NotImplementedOperation("rawDelete not implemented"))
    }
  }

}
