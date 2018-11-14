package com.emarsys.rdb.connector.common.models

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper.StringValue
import com.emarsys.rdb.connector.common.models.DataManipulation.{Criteria, Record, UpdateDefinition}
import com.emarsys.rdb.connector.common.models.Errors.{FailedValidation, SimpleSelectIsNotGroupableFormat}
import com.emarsys.rdb.connector.common.models.SimpleSelect.{AllField, TableName}
import com.emarsys.rdb.connector.common.models.TableSchemaDescriptors.{FieldModel, TableModel}
import com.emarsys.rdb.connector.common.models.ValidateDataManipulation.ValidationResult.InvalidOperationOnView
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class ConnectorSpec extends WordSpecLike with Matchers {

  implicit val executionCtx: ExecutionContext = ExecutionContext.global

  val tableName = "tableName"
  val viewName = "viewName"

  val defaultTimeout = 3.seconds

  val myConnector = new Connector {
    override implicit val executionContext: ExecutionContext = executionCtx

    override def close() = ???

    override def listTables() = Future.successful(Right(Seq(TableModel(tableName, false), TableModel(viewName, true))))

    override def listFields(table: String) = Future.successful(Right(Seq(FieldModel("a", ""), FieldModel("b", ""))))

    override def isOptimized(table: String, fields: Seq[String]) = Future.successful(Right(true))

    override protected def rawUpdate(tableName: String, definitions: Seq[DataManipulation.UpdateDefinition]): ConnectorResponse[Int] = Future.successful(Right(4))

    override protected def rawInsertData(tableName: String, data: Seq[Record]): ConnectorResponse[Int] = Future.successful(Right(4))

    override protected def rawReplaceData(tableName: String, data: Seq[Record]): ConnectorResponse[Int] = Future.successful(Right(4))

    override protected def rawUpsert(tableName: String, data: Seq[Record]): ConnectorResponse[Int] = Future.successful(Right(4))

    override protected def rawDelete(tableName: String, criteria: Seq[Criteria]): ConnectorResponse[Int] = Future.successful(Right(4))

  }

  "#update" should {

    "return ok" in {
      val definitions = Seq(UpdateDefinition(Map("a" -> StringValue("1")), Map("b" -> StringValue("2"))))
      Await.result(myConnector.update(tableName, definitions), defaultTimeout) shouldBe Right(4)
    }

    "return validation error" in {
      val definitions = Seq(UpdateDefinition(Map("a" -> StringValue("1")), Map("b" -> StringValue("2"))))
      Await.result(myConnector.update(viewName, definitions), defaultTimeout) shouldBe Left(FailedValidation(InvalidOperationOnView))
    }

  }

  "#insert" should {

    "return ok" in {
      val records = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      Await.result(myConnector.insertIgnore(tableName, records), defaultTimeout) shouldBe Right(4)
    }

    "return validation error" in {
      val records = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      Await.result(myConnector.insertIgnore(viewName, records), defaultTimeout) shouldBe Left(FailedValidation(InvalidOperationOnView))
    }

  }

  "#replace" should {

    "return ok" in {
      val records = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      Await.result(myConnector.replaceData(tableName, records), defaultTimeout) shouldBe Right(4)
    }

    "return validation error" in {
      val records = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      Await.result(myConnector.replaceData(viewName, records), defaultTimeout) shouldBe Left(FailedValidation(InvalidOperationOnView))
    }
  }

  "#upsert" should {

    "return ok" in {
      val records = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      Await.result(myConnector.upsert(tableName, records), defaultTimeout) shouldBe Right(4)
    }

    "return validation error" in {
      val records = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      Await.result(myConnector.upsert(viewName, records), defaultTimeout) shouldBe Left(FailedValidation(InvalidOperationOnView))
    }

  }

  "#delete" should {

    "return ok" in {
      val criteria = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      Await.result(myConnector.delete(tableName, criteria), defaultTimeout) shouldBe Right(4)
    }

    "return validation error" in {
      val criteria = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      Await.result(myConnector.delete(viewName, criteria), defaultTimeout) shouldBe Left(FailedValidation(InvalidOperationOnView))
    }
  }

  "#isErrorRetryable" should {
    "return false for any input" in {
      myConnector.isErrorRetryable(new Exception()) shouldBe false
    }
  }

  "#selectWithGroupLimit" should {

    val select = SimpleSelect(AllField, TableName("table"))

    "use simpleSelect if validator choose Simple" in {
      var calledWith: Seq[SimpleSelect] = Seq.empty
      val validator = new ValidateGroupLimitableQuery {
        override def groupLimitableQueryValidation(simpleSelect: SimpleSelect): ValidateGroupLimitableQuery.GroupLimitValidationResult = ValidateGroupLimitableQuery.GroupLimitValidationResult.Simple
      }
      val connector = new Connector {
        override implicit val executionContext: ExecutionContext = executionCtx
        override val groupLimitValidator = validator

        override def simpleSelect(select: SimpleSelect, timeout: FiniteDuration): ConnectorResponse[Source[Seq[String], NotUsed]] = {
          calledWith +:= select
          common.notImplementedOperation("simpleSelect")
        }

        override def close(): Future[Unit] = ???
      }

      connector.selectWithGroupLimit(select, 5, 10.minutes)
      calledWith.size shouldBe 1
      calledWith.head.limit shouldBe Some(5)
    }

    "use runSelectWithGroupLimit if validator choose Groupable" in {
      var calledWith: Seq[SimpleSelect] = Seq.empty
      val validator = new ValidateGroupLimitableQuery {
        override def groupLimitableQueryValidation(simpleSelect: SimpleSelect): ValidateGroupLimitableQuery.GroupLimitValidationResult = ValidateGroupLimitableQuery.GroupLimitValidationResult.Groupable(Seq.empty)
      }
      val connector = new Connector {
        override implicit val executionContext: ExecutionContext = executionCtx
        override val groupLimitValidator = validator

        override def runSelectWithGroupLimit(select: SimpleSelect, groupLimit: Int, references: Seq[String], timeout: FiniteDuration): ConnectorResponse[Source[Seq[String], NotUsed]] = {
          calledWith +:= select
          common.notImplementedOperation("simpleSelect")
        }

        override def close(): Future[Unit] = ???
      }

      connector.selectWithGroupLimit(select, 5, 10.minutes)
      calledWith.size shouldBe 1
      calledWith.head.limit shouldBe None
    }

    "drop error if validator choose NotGroupable" in {
      val validator = new ValidateGroupLimitableQuery {
        override def groupLimitableQueryValidation(simpleSelect: SimpleSelect): ValidateGroupLimitableQuery.GroupLimitValidationResult = ValidateGroupLimitableQuery.GroupLimitValidationResult.NotGroupable
      }
      val connector = new Connector {
        override implicit val executionContext: ExecutionContext = executionCtx
        override val groupLimitValidator = validator

        override def close(): Future[Unit] = ???
      }

      Await.result(connector.selectWithGroupLimit(select, 5, 10.minutes), 1.second) shouldBe Left(SimpleSelectIsNotGroupableFormat("SimpleSelect(AllField,TableName(table),None,None,None)"))
    }
  }

}
