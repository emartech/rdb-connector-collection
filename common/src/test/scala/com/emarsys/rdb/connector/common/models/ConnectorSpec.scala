package com.emarsys.rdb.connector.common.models

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper.StringValue
import com.emarsys.rdb.connector.common.models.DataManipulation.{Criteria, Record, UpdateDefinition}
import com.emarsys.rdb.connector.common.models.Errors.{
  FailedValidation,
  SimpleSelectIsNotGroupableFormat,
  SqlSyntaxError
}
import com.emarsys.rdb.connector.common.models.SimpleSelect.{AllField, TableName}
import com.emarsys.rdb.connector.common.models.ValidationResult.{InvalidOperationOnView, Valid}
import org.mockito.Answers
import org.mockito.ArgumentMatchers.{any, eq => eqTo}
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class ConnectorSpec extends WordSpecLike with Matchers with MockitoSugar {

  val tableName = "tableName"
  val viewName  = "viewName"

  trait TestScope extends Connector {

    val defaultTimeout = 3.seconds

    val validValidationResult          = Future.successful(Right(Valid))
    val invalidValidationResult        = Future.successful(Right(InvalidOperationOnView))
    val connectorError                 = Left(SqlSyntaxError("Oh, Snap!"))
    val connectorErrorValidationResult = Future.successful(connectorError)

    override val validator = mock[DataManipulationValidator](Answers.RETURNS_SMART_NULLS)

    override implicit val executionContext: ExecutionContext = global

    override def close() = ???

    override def listTables() = ???

    override def listFields(table: String) = ???

    override def isOptimized(table: String, fields: Seq[String]) = ???

    override protected def rawUpdate(
        tableName: String,
        definitions: Seq[DataManipulation.UpdateDefinition]
    ): ConnectorResponse[Int] = Future.successful(Right(4))

    override protected def rawInsertData(tableName: String, data: Seq[Record]): ConnectorResponse[Int] =
      Future.successful(Right(4))

    override protected def rawReplaceData(tableName: String, data: Seq[Record]): ConnectorResponse[Int] =
      Future.successful(Right(4))

    override protected def rawUpsert(tableName: String, data: Seq[Record]): ConnectorResponse[Int] =
      Future.successful(Right(4))

    override protected def rawDelete(tableName: String, criteria: Seq[Criteria]): ConnectorResponse[Int] =
      Future.successful(Right(4))

  }

  "#update" should {

    "return ok" in new TestScope {
      val definitions = Seq(UpdateDefinition(Map("a" -> StringValue("1")), Map("b" -> StringValue("2"))))
      when(
        validator.validateUpdateDefinition(eqTo(tableName), eqTo(definitions), any[Connector])(any[ExecutionContext])
      ) thenReturn validValidationResult

      Await.result(update(tableName, definitions), defaultTimeout) shouldBe Right(4)
    }

    "return validation error" in new TestScope {
      val definitions = Seq(UpdateDefinition(Map("a" -> StringValue("1")), Map("b" -> StringValue("2"))))
      when(
        validator.validateUpdateDefinition(eqTo(viewName), eqTo(definitions), any[Connector])(any[ExecutionContext])
      ) thenReturn invalidValidationResult
      Await.result(update(viewName, definitions), defaultTimeout) shouldBe Left(
        FailedValidation(InvalidOperationOnView)
      )
    }

    "return connector error unchanged" in new TestScope {
      val definitions = Seq(UpdateDefinition(Map("a" -> StringValue("1")), Map("b" -> StringValue("2"))))
      when(
        validator.validateUpdateDefinition(eqTo(viewName), eqTo(definitions), any[Connector])(any[ExecutionContext])
      ) thenReturn connectorErrorValidationResult

      Await.result(update(viewName, definitions), defaultTimeout) shouldBe connectorError
    }

  }

  "#insert" should {

    "return ok" in new TestScope {
      val records = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      when(
        validator.validateInsertData(eqTo(tableName), eqTo(records), any[Connector])(any[ExecutionContext])
      ) thenReturn validValidationResult
      Await.result(insertIgnore(tableName, records), defaultTimeout) shouldBe Right(4)
    }

    "return validation error" in new TestScope {
      val records = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      when(
        validator.validateInsertData(eqTo(viewName), eqTo(records), any[Connector])(any[ExecutionContext])
      ) thenReturn invalidValidationResult
      Await.result(insertIgnore(viewName, records), defaultTimeout) shouldBe Left(
        FailedValidation(InvalidOperationOnView)
      )
    }

    "return connector error unchanged" in new TestScope {
      val records = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      when(
        validator.validateInsertData(eqTo(viewName), eqTo(records), any[Connector])(any[ExecutionContext])
      ) thenReturn connectorErrorValidationResult

      Await.result(insertIgnore(viewName, records), defaultTimeout) shouldBe connectorError
    }

  }

  "#replace" should {

    "return ok" in new TestScope {
      val records = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      when(
        validator.validateInsertData(eqTo(tableName), eqTo(records), any[Connector])(any[ExecutionContext])
      ) thenReturn validValidationResult
      Await.result(replaceData(tableName, records), defaultTimeout) shouldBe Right(4)
    }

    "return validation error" in new TestScope {
      val records = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      when(
        validator.validateInsertData(eqTo(viewName), eqTo(records), any[Connector])(any[ExecutionContext])
      ) thenReturn invalidValidationResult
      Await.result(replaceData(viewName, records), defaultTimeout) shouldBe Left(
        FailedValidation(InvalidOperationOnView)
      )
    }

    "return connector error unchanged" in new TestScope {
      val records = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      when(
        validator.validateInsertData(eqTo(viewName), eqTo(records), any[Connector])(any[ExecutionContext])
      ) thenReturn connectorErrorValidationResult

      Await.result(replaceData(viewName, records), defaultTimeout) shouldBe connectorError
    }
  }

  "#upsert" should {

    "return ok" in new TestScope {
      val records = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      when(
        validator.validateInsertData(eqTo(tableName), eqTo(records), any[Connector])(any[ExecutionContext])
      ) thenReturn validValidationResult
      Await.result(upsert(tableName, records), defaultTimeout) shouldBe Right(4)
    }

    "return validation error" in new TestScope {
      val records = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      when(
        validator.validateInsertData(eqTo(viewName), eqTo(records), any[Connector])(any[ExecutionContext])
      ) thenReturn invalidValidationResult
      Await.result(upsert(viewName, records), defaultTimeout) shouldBe Left(
        FailedValidation(InvalidOperationOnView)
      )
    }

    "return connector error unchanged" in new TestScope {
      val records = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      when(
        validator.validateInsertData(eqTo(viewName), eqTo(records), any[Connector])(any[ExecutionContext])
      ) thenReturn connectorErrorValidationResult

      Await.result(upsert(viewName, records), defaultTimeout) shouldBe connectorError
    }
  }

  "#delete" should {

    "return ok" in new TestScope {
      val criteria = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      when(
        validator.validateDeleteCriteria(eqTo(tableName), eqTo(criteria), any[Connector])(any[ExecutionContext])
      ) thenReturn validValidationResult
      Await.result(delete(tableName, criteria), defaultTimeout) shouldBe Right(4)
    }

    "return validation error" in new TestScope {
      val criteria = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      when(
        validator.validateDeleteCriteria(eqTo(viewName), eqTo(criteria), any[Connector])(any[ExecutionContext])
      ) thenReturn invalidValidationResult
      Await.result(delete(viewName, criteria), defaultTimeout) shouldBe Left(
        FailedValidation(InvalidOperationOnView)
      )
    }

    "return connector error unchanged" in new TestScope {
      val criteria = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      when(
        validator.validateDeleteCriteria(eqTo(viewName), eqTo(criteria), any[Connector])(any[ExecutionContext])
      ) thenReturn connectorErrorValidationResult

      Await.result(delete(viewName, criteria), defaultTimeout) shouldBe connectorError
    }
  }

  "#isErrorRetryable" should {
    "return false for any input" in new TestScope {
      isErrorRetryable(new Exception()) shouldBe false
    }
  }

  "#selectWithGroupLimit" should {

    val select = SimpleSelect(AllField, TableName("table"))

    "use simpleSelect if validator choose Simple" in new TestScope {
      var calledWith: Seq[SimpleSelect] = Seq.empty
      val stubbedGroupLimitValidator = new ValidateGroupLimitableQuery {
        override def groupLimitableQueryValidation(
            simpleSelect: SimpleSelect
        ): ValidateGroupLimitableQuery.GroupLimitValidationResult =
          ValidateGroupLimitableQuery.GroupLimitValidationResult.Simple
      }
      override val groupLimitValidator = stubbedGroupLimitValidator
      override def simpleSelect(
          select: SimpleSelect,
          timeout: FiniteDuration
      ): ConnectorResponse[Source[Seq[String], NotUsed]] = {
        calledWith +:= select
        common.notImplementedOperation("simpleSelect")
      }

      selectWithGroupLimit(select, 5, 10.minutes)
      calledWith.size shouldBe 1
      calledWith.head.limit shouldBe Some(5)
    }

    "use runSelectWithGroupLimit if validator choose Groupable" in new TestScope {
      var calledWith: Seq[SimpleSelect] = Seq.empty
      val stubbedGroupLimitValidator = new ValidateGroupLimitableQuery {
        override def groupLimitableQueryValidation(
            simpleSelect: SimpleSelect
        ): ValidateGroupLimitableQuery.GroupLimitValidationResult =
          ValidateGroupLimitableQuery.GroupLimitValidationResult.Groupable(Seq.empty)
      }
      override val groupLimitValidator = stubbedGroupLimitValidator

      override def runSelectWithGroupLimit(
          select: SimpleSelect,
          groupLimit: Int,
          references: Seq[String],
          timeout: FiniteDuration
      ): ConnectorResponse[Source[Seq[String], NotUsed]] = {
        calledWith +:= select
        common.notImplementedOperation("simpleSelect")
      }

      selectWithGroupLimit(select, 5, 10.minutes)
      calledWith.size shouldBe 1
      calledWith.head.limit shouldBe None
    }

    "drop error if validator choose NotGroupable" in new TestScope {
      val stubbedGroupLimitValidator = new ValidateGroupLimitableQuery {
        override def groupLimitableQueryValidation(
            simpleSelect: SimpleSelect
        ): ValidateGroupLimitableQuery.GroupLimitValidationResult =
          ValidateGroupLimitableQuery.GroupLimitValidationResult.NotGroupable
      }
      override implicit val executionContext: ExecutionContext = global
      override val groupLimitValidator                         = stubbedGroupLimitValidator

      Await.result(selectWithGroupLimit(select, 5, 10.minutes), 1.second) shouldBe Left(
        SimpleSelectIsNotGroupableFormat("SimpleSelect(AllField,TableName(table),None,None,None)")
      )
    }
  }

}
