package com.emarsys.rdb.connector.common.models

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper.StringValue
import com.emarsys.rdb.connector.common.models.DataManipulation.{Criteria, Record, UpdateDefinition}
import com.emarsys.rdb.connector.common.models.Errors._
import com.emarsys.rdb.connector.common.models.SimpleSelect.{AllField, TableName}
import org.mockito.Answers
import org.mockito.Mockito._
import org.scalatest.concurrent.ScalaFutures
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.EitherValues

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class ConnectorSpec extends AnyWordSpecLike with Matchers with MockitoSugar with ScalaFutures with EitherValues {

  val tableName                      = "tableName"
  val viewName                       = "viewName"
  val defaultTimeout                 = 3.seconds
  val validValidationResult          = Future.successful(Right(()))
  val validationError                = Left(DatabaseError.validation(ErrorName.InvalidOperationOnView))
  val invalidValidationResult        = Future.successful(validationError)
  val connectorError                 = Left(DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SqlSyntaxError, "oh no"))
  val connectorErrorValidationResult = Future.successful(connectorError)
  val rawResponse                    = Right(4)

  trait TestScope extends Connector {

    override val validator = mock[DataManipulationValidator](Answers.RETURNS_SMART_NULLS)

    implicit override val executionContext: ExecutionContext = global

    override def close() = ???

    override def listTables() = ???

    override def listFields(table: String) = ???

    override def isOptimized(table: String, fields: Seq[String]) = ???

    override protected def rawUpdate(
        tableName: String,
        definitions: Seq[DataManipulation.UpdateDefinition]
    ): ConnectorResponse[Int] = Future.successful(rawResponse)

    override protected def rawInsertData(tableName: String, data: Seq[Record]): ConnectorResponse[Int] =
      Future.successful(rawResponse)

    override protected def rawReplaceData(tableName: String, data: Seq[Record]): ConnectorResponse[Int] =
      Future.successful(rawResponse)

    override protected def rawUpsert(tableName: String, data: Seq[Record]): ConnectorResponse[Int] =
      Future.successful(rawResponse)

    override protected def rawDelete(tableName: String, criteria: Seq[Criteria]): ConnectorResponse[Int] =
      Future.successful(rawResponse)

  }

  "#update" should {

    "return ok" in new TestScope {
      val definitions = Seq(UpdateDefinition(Map("a" -> StringValue("1")), Map("b" -> StringValue("2"))))
      when(validator.validateUpdateDefinition(tableName, definitions, this)(executionContext)) thenReturn validValidationResult

      update(tableName, definitions).futureValue shouldBe rawResponse
    }

    "return validation error" in new TestScope {
      val definitions = Seq(UpdateDefinition(Map("a" -> StringValue("1")), Map("b" -> StringValue("2"))))
      when(validator.validateUpdateDefinition(viewName, definitions, this)(executionContext)) thenReturn invalidValidationResult
      update(viewName, definitions).futureValue shouldBe validationError
    }

    "return connector error unchanged" in new TestScope {
      val definitions = Seq(UpdateDefinition(Map("a" -> StringValue("1")), Map("b" -> StringValue("2"))))
      when(validator.validateUpdateDefinition(viewName, definitions, this)(executionContext)) thenReturn connectorErrorValidationResult

      update(viewName, definitions).futureValue shouldBe connectorError
    }

  }

  "#insert" should {

    "return ok" in new TestScope {
      val records = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      when(validator.validateInsertData(tableName, records, this)(executionContext)) thenReturn validValidationResult
      insertIgnore(tableName, records).futureValue shouldBe Right(4)
    }

    "return validation error" in new TestScope {
      val records = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      when(validator.validateInsertData(viewName, records, this)(executionContext)) thenReturn invalidValidationResult
      insertIgnore(viewName, records).futureValue shouldBe validationError
    }

    "return connector error unchanged" in new TestScope {
      val records = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      when(validator.validateInsertData(viewName, records, this)(executionContext)) thenReturn connectorErrorValidationResult

      insertIgnore(viewName, records).futureValue shouldBe connectorError
    }

  }

  "#replace" should {

    "return ok" in new TestScope {
      val records = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      when(validator.validateInsertData(tableName, records, this)(executionContext)) thenReturn validValidationResult
      replaceData(tableName, records).futureValue shouldBe Right(4)
    }

    "return validation error" in new TestScope {
      val records = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      when(validator.validateInsertData(viewName, records, this)(executionContext)) thenReturn invalidValidationResult
      replaceData(viewName, records).futureValue shouldBe validationError
    }

    "return connector error unchanged" in new TestScope {
      val records = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      when(validator.validateInsertData(viewName, records, this)(executionContext)) thenReturn connectorErrorValidationResult

      replaceData(viewName, records).futureValue shouldBe connectorError
    }
  }

  "#upsert" should {

    "return ok" in new TestScope {
      val records = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      when(validator.validateInsertData(tableName, records, this)(executionContext)) thenReturn validValidationResult
      upsert(tableName, records).futureValue shouldBe Right(4)
    }

    "return validation error" in new TestScope {
      val records = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      when(validator.validateInsertData(viewName, records, this)(executionContext)) thenReturn invalidValidationResult
      upsert(viewName, records).futureValue shouldBe validationError
    }

    "return connector error unchanged" in new TestScope {
      val records = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      when(validator.validateInsertData(viewName, records, this)(executionContext)) thenReturn connectorErrorValidationResult

      upsert(viewName, records).futureValue shouldBe connectorError
    }
  }

  "#delete" should {

    "return ok" in new TestScope {
      val criteria = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      when(validator.validateDeleteCriteria(tableName, criteria, this)(executionContext)) thenReturn validValidationResult
      delete(tableName, criteria).futureValue shouldBe Right(4)
    }

    "return validation error" in new TestScope {
      val criteria = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      when(validator.validateDeleteCriteria(viewName, criteria, this)(executionContext)) thenReturn invalidValidationResult
      delete(viewName, criteria).futureValue shouldBe validationError
    }

    "return connector error unchanged" in new TestScope {
      val criteria = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      when(validator.validateDeleteCriteria(viewName, criteria, this)(executionContext)) thenReturn connectorErrorValidationResult

      delete(viewName, criteria).futureValue shouldBe connectorError
    }
  }

  "#isErrorRetryable" should {
    "return false for any input" in new TestScope {
      val databaseError = DatabaseError(ErrorCategory.Timeout, ErrorName.QueryTimeout, "")
      isErrorRetryable(databaseError) shouldBe false
    }
  }

  "#selectWithGroupLimit" should {

    val select = SimpleSelect(AllField, TableName("table"))

    "use simpleSelect if validator choose Simple" in new TestScope {
      var calledWith: Seq[SimpleSelect] = Seq.empty
      val stubbedGroupLimitValidator = new ValidateGroupLimitableQuery {
        override def validate(
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
        override def validate(
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
        override def validate(
            simpleSelect: SimpleSelect
        ): ValidateGroupLimitableQuery.GroupLimitValidationResult =
          ValidateGroupLimitableQuery.GroupLimitValidationResult.NotGroupable
      }
      implicit override val executionContext: ExecutionContext = global
      override val groupLimitValidator                         = stubbedGroupLimitValidator

      selectWithGroupLimit(select, 5, 10.minutes).futureValue shouldBe Left(
        DatabaseError(ErrorCategory.Internal, ErrorName.SimpleSelectIsNotGroupableFormat, select.toString)
      )
    }
  }
}
