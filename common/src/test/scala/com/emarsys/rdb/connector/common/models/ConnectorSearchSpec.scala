package com.emarsys.rdb.connector.common.models

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.DataManipulation.Criteria
import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper._
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName}
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import org.mockito.Answers
import org.mockito.ArgumentMatchers.{any, eq => eqTo}
import org.mockito.Mockito.{never, spy, verify, when}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class ConnectorSearchSpec extends WordSpecLike with Matchers with MockitoSugar {

  val tableName = "tableName"
  val viewName  = "viewName"

  val defaultTimeout = 3.seconds
  val sqlTimeout     = 3.seconds

  trait TestScope {
    val validValidationResult          = Future.successful(Right(()))
    val validationError                = Left(DatabaseError.validation(ErrorName.InvalidOperationOnView))
    val invalidValidationResult        = Future.successful(validationError)
    val connectorError                 = Left(DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SqlSyntaxError, "Oh, Snap!"))
    val connectorErrorValidationResult = Future.successful(connectorError)
    val validator                      = mock[DataManipulationValidator](Answers.RETURNS_SMART_NULLS)

    val myConnector = spy(new TestConnector(validator))
  }

  class TestConnector(override val validator: DataManipulationValidator) extends Connector {
    override implicit val executionContext: ExecutionContext = global

    override def close() = ???

    override def listTables() = ???

    override def listFields(table: String) = ???

    override def isOptimized(table: String, fields: Seq[String]) = ???

    override def simpleSelect(
        select: SimpleSelect,
        timeout: FiniteDuration
    ): ConnectorResponse[Source[Seq[String], NotUsed]] =
      Future.successful(Right(Source(List(Seq("head")))))
  }

  "#search" should {

    "return ok - string value" in new TestScope {
      val criteria: Criteria = Map("a" -> StringValue("1"))
      when(validator.validateSearchCriteria(eqTo(tableName), eqTo(criteria), any[Connector])(any[ExecutionContext])) thenReturn validValidationResult

      Await.result(myConnector.search(tableName, criteria, Some(1), sqlTimeout), defaultTimeout) shouldBe a[Right[_, _]]
      verify(myConnector).simpleSelect(
        SimpleSelect(
          AllField,
          TableName(tableName),
          Some(And(Seq(EqualToValue(FieldName("a"), Value("1"))))),
          Some(1)
        ),
        sqlTimeout
      )
    }

    "return ok - int value" in new TestScope {
      val criteria: Criteria = Map("a" -> IntValue(1))
      when(validator.validateSearchCriteria(eqTo(tableName), eqTo(criteria), any[Connector])(any[ExecutionContext])) thenReturn validValidationResult

      Await.result(myConnector.search(tableName, criteria, Some(1), sqlTimeout), defaultTimeout) shouldBe a[Right[_, _]]
      verify(myConnector).simpleSelect(
        SimpleSelect(
          AllField,
          TableName(tableName),
          Some(And(Seq(EqualToValue(FieldName("a"), Value("1"))))),
          Some(1)
        ),
        sqlTimeout
      )
    }

    "return ok - bigdecimal value" in new TestScope {
      val criteria: Criteria = Map("a" -> BigDecimalValue(1))
      when(validator.validateSearchCriteria(eqTo(tableName), eqTo(criteria), any[Connector])(any[ExecutionContext])) thenReturn validValidationResult

      Await.result(myConnector.search(tableName, criteria, Some(1), sqlTimeout), defaultTimeout) shouldBe a[Right[_, _]]
      verify(myConnector).simpleSelect(
        SimpleSelect(
          AllField,
          TableName(tableName),
          Some(And(Seq(EqualToValue(FieldName("a"), Value("1"))))),
          Some(1)
        ),
        sqlTimeout
      )
    }

    "return ok - boolean value" in new TestScope {
      val criteria: Criteria = Map("a" -> BooleanValue(true))
      when(validator.validateSearchCriteria(eqTo(tableName), eqTo(criteria), any[Connector])(any[ExecutionContext])) thenReturn validValidationResult

      Await.result(myConnector.search(tableName, criteria, Some(1), sqlTimeout), defaultTimeout) shouldBe a[Right[_, _]]
      verify(myConnector).simpleSelect(
        SimpleSelect(
          AllField,
          TableName(tableName),
          Some(And(Seq(EqualToValue(FieldName("a"), Value("true"))))),
          Some(1)
        ),
        sqlTimeout
      )
    }

    "return ok - null value" in new TestScope {
      val criteria: Criteria = Map("a" -> NullValue)
      when(validator.validateSearchCriteria(eqTo(tableName), eqTo(criteria), any[Connector])(any[ExecutionContext])) thenReturn validValidationResult

      Await.result(myConnector.search(tableName, criteria, Some(1), sqlTimeout), defaultTimeout) shouldBe a[Right[_, _]]
      verify(myConnector).simpleSelect(
        SimpleSelect(
          AllField,
          TableName(tableName),
          Some(And(Seq(IsNull(FieldName("a"))))),
          Some(1)
        ),
        sqlTimeout
      )
    }

    "return validation failure if validation fails" in new TestScope {
      val criteria: Criteria = Map("a" -> NullValue)
      when(validator.validateSearchCriteria(eqTo(tableName), eqTo(criteria), any[Connector])(any[ExecutionContext])) thenReturn invalidValidationResult

      Await.result(myConnector.search(tableName, criteria, Some(1), sqlTimeout), defaultTimeout) shouldBe validationError

      verify(myConnector, never()).simpleSelect(any[SimpleSelect], any[FiniteDuration])
    }

    "return Connector error untouched" in new TestScope {
      val criteria: Criteria = Map("a" -> StringValue("1"))
      when(validator.validateSearchCriteria(eqTo(tableName), eqTo(criteria), any[Connector])(any[ExecutionContext])) thenReturn connectorErrorValidationResult

      Await.result(myConnector.search(tableName, criteria, Some(1), sqlTimeout), defaultTimeout) shouldBe connectorError
      verify(myConnector, never()).simpleSelect(any[SimpleSelect], any[FiniteDuration])
    }
  }
}
