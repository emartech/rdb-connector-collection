package com.emarsys.rdb.connector.common.models

import com.emarsys.rdb.connector.common.ConnectorResponseET
import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper.StringValue
import com.emarsys.rdb.connector.common.models.DataManipulation.UpdateDefinition
import com.emarsys.rdb.connector.common.models.Errors.ConnectorError
import com.emarsys.rdb.connector.common.models.TableSchemaDescriptors.{FieldModel, TableModel}
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{EitherValues, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class DataValidatorSpec extends WordSpecLike with Matchers with MockitoSugar with EitherValues {
  implicit val executionContext: ExecutionContext = ExecutionContext.global

  val tableName = "tableName"
  val viewName  = "viewName"
  val availableTables = Future.successful(
    Right(
      Seq(
        TableModel(tableName, isView = false),
        TableModel(viewName, isView = true)
      )
    )
  )

  class ValidatorScope {
    val connector = mock[Connector]

    def initTables(): Unit = {
      when(connector.listTables()) thenReturn availableTables
    }

    def initIsOptimized(_tableName: String, fieldNames: Seq[String], isOptimized: Boolean): Unit = {
      when(connector.isOptimized(_tableName, fieldNames)) thenReturn Future.successful(Right(isOptimized))
    }

    def initFieldsForTable(_tableName: String, fieldNames: Seq[String]): Unit = {
      when(connector.listFields(_tableName)) thenReturn
        Future.successful(Right(fieldNames.map(FieldModel(_, ""))))
    }
  }

  private def await(f: ConnectorResponseET[ValidationResult]): Either[ConnectorError, ValidationResult] = {
    Await.result(f.value, 3.seconds)
  }

  "#validateEmptyCriteria" should {
    "return Valid if the data is not empty" in new ValidatorScope {
      await(DataValidator.validateEmptyCriteria(Map("field1" -> StringValue("value1")))).right.value shouldBe
        ValidationResult.Valid
    }

    "return EmptyData if the data is empty" in new ValidatorScope {
      await(DataValidator.validateEmptyCriteria(Map())).right.value shouldBe
        ValidationResult.EmptyData
    }
  }

  "#validateFieldExistence" should {
    "return Valid if all fields exist" in new ValidatorScope {
      initFieldsForTable(tableName, Seq("field1", "field2"))

      await(DataValidator.validateFieldExistence(tableName, Set("field1", "field2"), connector)).right.value shouldBe
        ValidationResult.Valid
    }

    "return NonExistingFields if some of the fields not exist" in new ValidatorScope {
      initFieldsForTable(tableName, Seq("field1", "field2"))

      await(DataValidator.validateFieldExistence(tableName, Set("field99", "field2"), connector)).right.value shouldBe
        ValidationResult.NonExistingFields(Set("field99"))
    }
  }

  "#validateTableExists" should {
    "return Valid if the table exists as table" in new ValidatorScope {
      initTables()

      await(DataValidator.validateTableExists(tableName, connector)).right.value shouldBe
        ValidationResult.Valid
    }

    "return Valid if the table exists as view" in new ValidatorScope {
      initTables()

      await(DataValidator.validateTableExists(viewName, connector)).right.value shouldBe
        ValidationResult.Valid
    }

    "return NonExistingTable if the table not exists neither as table or view" in new ValidatorScope {
      initTables()

      await(DataValidator.validateTableExists("unknown_table", connector)).right.value shouldBe
        ValidationResult.NonExistingTable
    }
  }

  "#validateTableExistsAndNotView" should {
    "return Valid if the table exists as table" in new ValidatorScope {
      initTables()

      await(DataValidator.validateTableExistsAndNotView(tableName, connector)).right.value shouldBe
        ValidationResult.Valid
    }

    "return InvalidOperationOnView if the table exists as view" in new ValidatorScope {
      initTables()

      await(DataValidator.validateTableExistsAndNotView(viewName, connector)).right.value shouldBe
        ValidationResult.InvalidOperationOnView
    }

    "return NonExistingTable if the table not exists neither as table or view" in new ValidatorScope {
      initTables()

      await(DataValidator.validateTableExistsAndNotView("unknown_table", connector)).right.value shouldBe
        ValidationResult.NonExistingTable
    }
  }

  "#validateUpdateFields" should {

    val updateData = Seq(
      UpdateDefinition(
        search = Map("search_field11" -> StringValue("search_v11"), "search_field12" -> StringValue("search_v12")),
        update = Map("update_field11" -> StringValue("update_v11"), "update_field12" -> StringValue("update_v12"))
      ),
      UpdateDefinition(
        search = Map("ud2s_k_1" -> StringValue("ud2s_v_1"), "ud2s_k_2" -> StringValue("ud2s_v_2")),
        update = Map("ud2u_k_3" -> StringValue("ud2u_v_1"))
      )
    )

    "return Valid if everything is correct" in new ValidatorScope {
      initFieldsForTable(tableName, Seq("search_field11", "search_field12", "update_field11", "update_field12"))
      initIsOptimized(tableName, Seq("search_field11", "search_field12"), isOptimized = true)

      await(DataValidator.validateUpdateFields(tableName, updateData, connector)).right.value shouldBe
        ValidationResult.Valid
    }

    "return EmptyData if the update data is empty" in new ValidatorScope {
      initFieldsForTable(tableName, Seq("search_field11", "search_field12", "update_field11", "update_field12"))
      initIsOptimized(tableName, Seq("search_field11", "search_field12"), isOptimized = true)

      await(DataValidator.validateUpdateFields(tableName, Seq(), connector)).right.value shouldBe
        ValidationResult.EmptyData
    }

    "return NonExistingFields if not all keys of first UpdateDefinition exists as field" in new ValidatorScope {
      initFieldsForTable(tableName, Seq("search_field11", "update_field12"))
      initIsOptimized(tableName, Seq("search_field11", "search_field12"), isOptimized = true)

      await(DataValidator.validateUpdateFields(tableName, updateData, connector)).right.value shouldBe
        ValidationResult.NonExistingFields(Set("search_field12", "update_field11"))
    }

    "return NoIndexOnFields if not all keys of search of first UpdateDefinition optimized as field" in new ValidatorScope {
      initFieldsForTable(tableName, Seq("search_field11", "search_field12", "update_field11", "update_field12"))
      initIsOptimized(tableName, Seq("search_field11", "search_field12"), isOptimized = false)

      await(DataValidator.validateUpdateFields(tableName, updateData, connector)).right.value shouldBe
        ValidationResult.NoIndexOnFields
    }
  }

  "#validateIndices" should {
    "return Valid if the fields are optimized" in new ValidatorScope {
      initIsOptimized(tableName, Seq("field1", "field2"), isOptimized = true)

      await(DataValidator.validateIndices(tableName, Set("field1", "field2"), connector)).right.value shouldBe
        ValidationResult.Valid
    }

    "return NoIndexOnFields if the fields are not optimized" in new ValidatorScope {
      initIsOptimized(tableName, Seq("field1", "field2"), isOptimized = false)

      await(DataValidator.validateIndices(tableName, Set("field1", "field2"), connector)).right.value shouldBe
        ValidationResult.NoIndexOnFields
    }
  }

  "#validateFormat" should {
    val record1            = Map("k1" -> StringValue("r1_v"))
    val record2            = Map("k1" -> StringValue("r2_v"))
    val record3            = Map("k1" -> StringValue("r3_v"))
    val recordDifferentKey = Map("k1" -> StringValue("r4_v1"), "k2" -> StringValue("r4_v2"))

    "return TooManyRows if there are too many data" in new ValidatorScope {
      await(DataValidator.validateFormat(Seq(record1, record2, record3), 2)).right.value shouldBe
        ValidationResult.TooManyRows
    }

    "return EmptyData if the data is empty" in new ValidatorScope {
      await(DataValidator.validateFormat(Seq(), 2)).right.value shouldBe
        ValidationResult.EmptyData
    }

    "return DifferentFields if the data is empty" in new ValidatorScope {
      await(DataValidator.validateFormat(Seq(record1, record2, recordDifferentKey), 3)).right.value shouldBe
        ValidationResult.DifferentFields
    }

    "return Valid everything is ok" in new ValidatorScope {
      await(DataValidator.validateFormat(Seq(record1, record2, record3), 3)).right.value shouldBe
        ValidationResult.Valid
    }
  }

  "#validateUpdateFormat" should {
    val updateData1 = UpdateDefinition(
      search = Map("field1" -> StringValue("value11"), "field2" -> StringValue("value12")),
      update = Map("field3" -> StringValue("value13"))
    )
    val updateData2 = UpdateDefinition(
      search = Map("field1" -> StringValue("value21"), "field2" -> StringValue("value22")),
      update = Map("field3" -> StringValue("value23"))
    )
    val updateData3 = UpdateDefinition(
      search = Map("field1" -> StringValue("value31"), "field2" -> StringValue("value32")),
      update = Map("field3" -> StringValue("value33"))
    )

    "return TooManyRows if there are too many data" in new ValidatorScope {
      await(DataValidator.validateUpdateFormat(Seq(updateData1, updateData2, updateData3), 2)).right.value shouldBe
        ValidationResult.TooManyRows
    }

    "return EmptyData if the data is empty" in new ValidatorScope {
      await(DataValidator.validateUpdateFormat(Seq(), 2)).right.value shouldBe
        ValidationResult.EmptyData
    }

    "return EmptyCriteria if there is an UpdateDefinition with empty criteria" in new ValidatorScope {
      val updateDataWithEmptySearch = UpdateDefinition(
        search = Map(),
        update = Map("k" -> StringValue("v"))
      )
      val data = Seq(updateData1, updateData2, updateDataWithEmptySearch)
      await(DataValidator.validateUpdateFormat(data, 3)).right.value shouldBe
        ValidationResult.EmptyCriteria
    }

    "return EmptyData if there is an UpdateDefinition with empty criteria" in new ValidatorScope {
      val updateDataWithEmptyUpdate = UpdateDefinition(
        search = Map("k" -> StringValue("v")),
        update = Map()
      )
      val data = Seq(updateData1, updateData2, updateDataWithEmptyUpdate)
      await(DataValidator.validateUpdateFormat(data, 3)).right.value shouldBe
        ValidationResult.EmptyData
    }

    "return DifferentFields if not all the search keys are the same" in new ValidatorScope {
      val updateDataWithDifferentSeach = UpdateDefinition(
        search = Map("field1" -> StringValue("value31"), "field2_different" -> StringValue("value32")),
        update = Map("field3" -> StringValue("value33"))
      )
      val data = Seq(updateData1, updateData2, updateDataWithDifferentSeach)
      await(DataValidator.validateUpdateFormat(data, 3)).right.value shouldBe
        ValidationResult.DifferentFields
    }

    "return DifferentFields if not all the update keys are the same" in new ValidatorScope {
      val updateDataWithDifferentSeach = UpdateDefinition(
        search = Map("field1"           -> StringValue("value31"), "field2" -> StringValue("value32")),
        update = Map("field3_different" -> StringValue("value33"))
      )
      val data = Seq(updateData1, updateData2, updateDataWithDifferentSeach)
      await(DataValidator.validateUpdateFormat(data, 3)).right.value shouldBe
        ValidationResult.DifferentFields
    }

    "return Valid if everything is ok" in new ValidatorScope {
      val data = Seq(updateData1, updateData2, updateData3)
      await(DataValidator.validateUpdateFormat(data, 3)).right.value shouldBe
        ValidationResult.Valid
    }
  }

}
