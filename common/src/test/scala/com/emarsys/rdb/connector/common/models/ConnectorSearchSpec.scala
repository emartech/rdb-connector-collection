package com.emarsys.rdb.connector.common.models

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.DataManipulation.Criteria
import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper._
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import com.emarsys.rdb.connector.common.models.TableSchemaDescriptors.{FieldModel, TableModel}
import org.mockito.Mockito.{reset, spy, verify}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class ConnectorSearchSpec extends WordSpecLike with Matchers with MockitoSugar with BeforeAndAfterEach {

  implicit val executionCtx: ExecutionContext = ExecutionContext.global

  val tableName = "tableName"
  val viewName  = "viewName"

  val defaultTimeout = 3.seconds
  val sqlTimeout     = 3.seconds

  class TestConnector extends Connector {
    override implicit val executionContext: ExecutionContext = executionCtx

    override def close() = ???

    override def listTables() = Future.successful(Right(Seq(TableModel(tableName, false), TableModel(viewName, true))))

    override def listFields(table: String) = Future.successful(Right(Seq(FieldModel("a", ""), FieldModel("b", ""))))

    override def isOptimized(table: String, fields: Seq[String]) = Future.successful(Right(true))

    override def simpleSelect(
        select: SimpleSelect,
        timeout: FiniteDuration
    ): ConnectorResponse[Source[Seq[String], NotUsed]] = Future.successful(Right(Source(List(Seq("head")))))
  }

  val myConnector = spy(new TestConnector())

  override def beforeEach() = {
    reset(myConnector)
  }

  "#search" should {

    "return ok - string value" in {
      val criteria: Criteria = Map("a" -> StringValue("1"))
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

    "return ok - int value" in {
      val criteria: Criteria = Map("a" -> IntValue(1))
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

    "return ok - bigdecimal value" in {
      val criteria: Criteria = Map("a" -> BigDecimalValue(1))
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

    "return ok - boolean value" in {
      val criteria: Criteria = Map("a" -> BooleanValue(true))
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

    "return ok - null value" in {
      val criteria: Criteria = Map("a" -> NullValue)
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
  }
}
