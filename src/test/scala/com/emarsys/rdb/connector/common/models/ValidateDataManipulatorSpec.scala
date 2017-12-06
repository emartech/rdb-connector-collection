package com.emarsys.rdb.connector.common.models

import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper.StringValue
import com.emarsys.rdb.connector.common.models.DataManipulation.{FieldValueWrapper, UpdateDefinition}
import com.emarsys.rdb.connector.common.models.Errors.TableNotFound
import com.emarsys.rdb.connector.common.models.TableSchemaDescriptors.{FieldModel, TableModel}
import com.emarsys.rdb.connector.common.models.ValidateDataManipulation.ValidationResult
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.{Await, ExecutionContext, Future}
import concurrent.duration._
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._

class ValidateDataManipulatorSpec extends WordSpecLike with Matchers with MockitoSugar{

  implicit val executionContext: ExecutionContext = ExecutionContext.global

  val defaultTimeout = 3.seconds
  val tableName = "tableName"
  val viewName = "viewName"

  class ValidatorScope {
    val connector = mock[Connector]
  }

  "#validateUpdateData" should {

      "return valid if everything is ok" in new ValidatorScope {
        when(connector.listFields(tableName)).thenReturn(Future.successful(Right(Seq(FieldModel("a", ""), FieldModel("b", "")))))
        when(connector.listTables()).thenReturn(Future.successful(Right(Seq(TableModel(tableName, false), TableModel(viewName, true)))))
        when(connector.isOptimized(any[String], any[Seq[String]])).thenReturn(Future.successful(Right(true)))

        val updateData = Seq(UpdateDefinition(Map("a" -> StringValue("1")), Map("b" -> StringValue("2"))))

        val validationResult = Await.result(ValidateDataManipulation.validateUpdateDefinition(tableName, updateData, connector), defaultTimeout)

        validationResult shouldBe ValidationResult.Valid
      }

      "return empty if nothing specified to update" in new ValidatorScope {
        val updateData = Seq[UpdateDefinition]()

        val validationResult = Await.result(ValidateDataManipulation.validateUpdateDefinition(tableName, updateData, connector), defaultTimeout)

        validationResult shouldBe ValidationResult.EmptyData
      }

      "return error if number of rows exceeds 1000" in new ValidatorScope {
        val updateData =  (1 to 1001).map(_ => UpdateDefinition(Map[String, FieldValueWrapper](), Map[String, FieldValueWrapper]()))

        val validationResult = Await.result(ValidateDataManipulation.validateUpdateDefinition(tableName, updateData, connector), defaultTimeout)

        validationResult shouldBe ValidationResult.TooManyRows
      }

      "return error for empty search" in new ValidatorScope {
        val updateData =  Seq(UpdateDefinition(Map[String, FieldValueWrapper](), Map("a" -> StringValue("1"))))

        val validationResult = Await.result(ValidateDataManipulation.validateUpdateDefinition(tableName, updateData, connector), defaultTimeout)

        validationResult shouldBe ValidationResult.EmptyCriteria
      }

      "return error for empty data" in new ValidatorScope {
        val updateData =  Seq(UpdateDefinition(Map("a" -> StringValue("1")), Map[String, FieldValueWrapper]()))

        val validationResult = Await.result(ValidateDataManipulation.validateUpdateDefinition(tableName, updateData, connector), defaultTimeout)

        validationResult shouldBe ValidationResult.EmptyData
      }

      "return error if not all criteria contains the same fields" in new ValidatorScope {
        val updateData =  Seq(UpdateDefinition(Map("a" -> StringValue("1"), "b" -> StringValue("2")), Map("c" -> StringValue("3"))), UpdateDefinition(Map("a" -> StringValue("1"), "x" -> StringValue("100")), Map("c" -> StringValue("3"))))

        val validationResult = Await.result(ValidateDataManipulation.validateUpdateDefinition(tableName, updateData, connector), defaultTimeout)

        validationResult shouldBe ValidationResult.DifferentFields
      }

      "return error if not all update record contains the same fields" in new ValidatorScope {
        when(connector.listTables()).thenReturn(Future.successful(Right(Seq(TableModel(tableName, false), TableModel(viewName, true)))))

        val updateData =  Seq(UpdateDefinition(Map("a" -> StringValue("1"), "b" -> StringValue("2")), Map("a" -> StringValue("1"), "b" -> StringValue("3"))), UpdateDefinition(Map("a" -> StringValue("1"), "x" -> StringValue("100")), Map("c" -> StringValue("3"))))

        val validationResult = Await.result(ValidateDataManipulation.validateUpdateDefinition(tableName, updateData, connector), defaultTimeout)

        validationResult shouldBe ValidationResult.DifferentFields
      }

      "return error if not all criteria fields present in the database table" in new ValidatorScope {
        when(connector.listTables()).thenReturn(Future.successful(Right(Seq(TableModel(tableName, false), TableModel(viewName, true)))))
        when(connector.listFields(tableName)).thenReturn(Future.successful(Right(Seq(FieldModel("exists",""), FieldModel("existsToo", "")))))

        val updateData = Seq(UpdateDefinition(Map("notExists" -> StringValue("b"), "exists" -> StringValue("2"), "notExistsEither" -> StringValue("whatever")), Map("exists" -> StringValue("value"))))

        val validationResult = Await.result(ValidateDataManipulation.validateUpdateDefinition(tableName, updateData, connector), defaultTimeout)
        validationResult shouldBe ValidationResult.NonExistingFields(Set("notExists", "notExistsEither"))
      }

      "return error if not all update data fields present in the database table" in new ValidatorScope {
        when(connector.listTables()).thenReturn(Future.successful(Right(Seq(TableModel(tableName, false), TableModel(viewName, true)))))
        when(connector.listFields(tableName)).thenReturn(Future.successful(Right(Seq(FieldModel("exists",""), FieldModel("existsToo", "")))))

        val updateData = Seq(UpdateDefinition(Map("exists" -> StringValue("a")), Map("exists" -> StringValue("value"), "notExists" -> StringValue("notValueHaha"))))

        val validationResult = Await.result(ValidateDataManipulation.validateUpdateDefinition(tableName, updateData, connector), defaultTimeout)
        validationResult shouldBe ValidationResult.NonExistingFields(Set("notExists"))
      }

      "return error if criteria fields has no indices" in new ValidatorScope {
        when(connector.listTables()).thenReturn(Future.successful(Right(Seq(TableModel(tableName, false), TableModel(viewName, true)))))
        when(connector.listFields(tableName)).thenReturn(Future.successful(Right(Seq(FieldModel("not_index",""), FieldModel("a", "")))))
        when(connector.isOptimized(tableName, Seq("not_index"))).thenReturn(Future.successful(Right(false)))

        val updateData = Seq(UpdateDefinition(Map("not_index" -> StringValue("a")), Map("a" -> StringValue("1"))))

        val validationResult = Await.result(ValidateDataManipulation.validateUpdateDefinition(tableName, updateData, connector), defaultTimeout)
        validationResult shouldBe ValidationResult.NoIndexOnFields
      }

      "return error if we want an operation on a view" in new ValidatorScope {
        when(connector.listTables()).thenReturn(Future.successful(Right(Seq(TableModel(tableName, false), TableModel(viewName, true)))))
        val updateData = Seq(UpdateDefinition(Map("not_index" -> StringValue("a")), Map("a" -> StringValue("1"))))
        val validationResult = Await.result(ValidateDataManipulation.validateUpdateDefinition(viewName, updateData, connector), defaultTimeout)
        validationResult shouldBe ValidationResult.InvalidOperationOnView
      }
    }

  "#validateInsertData" should {

    "return valid if number of rows is below or exactly 1000" in new ValidatorScope {
      when(connector.listTables()).thenReturn(Future.successful(Right(Seq(TableModel(tableName, false), TableModel(viewName, true)))))
      when(connector.listFields(tableName)).thenReturn(Future.successful(Right(Seq(FieldModel("a", "text"), FieldModel("b", "text")))))


      val data =  (1 to 1000).map(_ => Map("a" -> StringValue("1")))

      val validationResult = Await.result(ValidateDataManipulation.validateInsertData(tableName, data, connector), defaultTimeout)

      validationResult shouldBe ValidationResult.Valid
    }

    "return error if number of rows exceeds 1000" in new ValidatorScope {
      when(connector.listTables()).thenReturn(Future.successful(Right(Seq(TableModel(tableName, false), TableModel(viewName, true)))))
      when(connector.listFields(tableName)).thenReturn(Future.successful(Right(Seq(FieldModel("a", "text"), FieldModel("b", "text")))))

      val data =  (1 to 1001).map(_ => Map("a" -> StringValue("1")))

      val validationResult = Await.result(ValidateDataManipulation.validateInsertData(tableName, data, connector), defaultTimeout)

      validationResult shouldBe ValidationResult.TooManyRows
    }

    "return valid if all records contains the same fields" in new ValidatorScope {
      when(connector.listTables()).thenReturn(Future.successful(Right(Seq(TableModel(tableName, false), TableModel(viewName, true)))))
      when(connector.listFields(tableName)).thenReturn(Future.successful(Right(Seq(FieldModel("a", "text"), FieldModel("b", "text")))))

      val data = Seq(Map("a" -> StringValue("b"), "a" -> StringValue("c")))
      val validationResult = Await.result(ValidateDataManipulation.validateInsertData(tableName, data, connector), defaultTimeout)
      validationResult shouldBe ValidationResult.Valid
    }

    "return valid if all records contains the same fields but in different order" in new ValidatorScope {
      when(connector.listTables()).thenReturn(Future.successful(Right(Seq(TableModel(tableName, false), TableModel(viewName, true)))))
      when(connector.listFields(tableName)).thenReturn(Future.successful(Right(Seq(FieldModel("a", "text"), FieldModel("b", "text")))))

      val data = Seq(Map("a" -> StringValue("1"), "b" -> StringValue("2")), Map("b" -> StringValue("3"), "a" -> StringValue("4")))

      val validationResult = Await.result(ValidateDataManipulation.validateInsertData(tableName, data, connector), defaultTimeout)
      validationResult shouldBe ValidationResult.Valid
    }

    "return valid for empty array" in new ValidatorScope {
      when(connector.listTables()).thenReturn(Future.successful(Right(Seq(TableModel(tableName, false), TableModel(viewName, true)))))
      when(connector.listFields(tableName)).thenReturn(Future.successful(Right(Seq(FieldModel("a", "text"), FieldModel("b", "text")))))

      val validationResult = Await.result(ValidateDataManipulation.validateInsertData(tableName, Seq(), connector), defaultTimeout)

      validationResult shouldBe ValidationResult.Valid
    }

    "return error if not all records contains the same fields" in new ValidatorScope {
      when(connector.listTables()).thenReturn(Future.successful(Right(Seq(TableModel(tableName, false), TableModel(viewName, true)))))
      when(connector.listFields(tableName)).thenReturn(Future.successful(Right(Seq(FieldModel("a", "text"), FieldModel("b", "text")))))

      val data: Seq[Map[String, StringValue]] = Seq(Map("a" -> StringValue("b")), Map())
      val validationResult = Await.result(ValidateDataManipulation.validateInsertData(tableName, data, connector), defaultTimeout)

      validationResult shouldBe ValidationResult.DifferentFields
    }

    "return error if not all fields present in the database table" in new ValidatorScope {
      when(connector.listTables()).thenReturn(Future.successful(Right(Seq(TableModel(tableName, false), TableModel(viewName, true)))))
      when(connector.listFields(tableName)).thenReturn(Future.successful(Right(Seq(FieldModel("exists", "text"), FieldModel("exists2", "text")))))

      val data: Seq[Map[String, StringValue]] = Seq(Map("notExists" -> StringValue("b"), "exists" -> StringValue("2"), "notExistsEither" -> StringValue("whatever")))
      val validationResult = Await.result(ValidateDataManipulation.validateInsertData(tableName, data, connector), defaultTimeout)
      validationResult shouldBe ValidationResult.NonExistingFields(Set("notExists", "notExistsEither"))
    }

    "return error if table not exists" in new ValidatorScope {
      when(connector.listTables()).thenReturn(Future.successful(Right(Seq(TableModel(tableName, false), TableModel(viewName, true)))))
      when(connector.listFields(tableName)).thenReturn(Future.successful(Left(TableNotFound(tableName))))

      val data = Seq(Map("a" -> StringValue("b")), Map("a" -> StringValue("c")))
      val validationResult = Await.result(ValidateDataManipulation.validateInsertData(tableName, data, connector), defaultTimeout)
      validationResult shouldBe ValidationResult.NonExistingTable
    }

    "return error if we want an operation on a view" in new ValidatorScope {
      when(connector.listTables()).thenReturn(Future.successful(Right(Seq(TableModel(tableName, false), TableModel(viewName, true)))))
      when(connector.listFields(viewName)).thenReturn(Future.successful(Left(TableNotFound(tableName))))

      val data = Seq(Map("a" -> StringValue("b")), Map("a" -> StringValue("c")))
      val validationResult = Await.result(ValidateDataManipulation.validateInsertData(viewName, data, connector), defaultTimeout)
      validationResult shouldBe ValidationResult.InvalidOperationOnView
    }

  }
}
