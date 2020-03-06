package com.emarsys.rdb.connector.common.models

import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper.StringValue
import com.emarsys.rdb.connector.common.models.DataManipulation.UpdateDefinition
import com.emarsys.rdb.connector.common.models.Errors._
import com.emarsys.rdb.connector.common.models.TableSchemaDescriptors.{FieldModel, TableModel}
import org.mockito.Mockito._
import org.scalatest.EitherValues
import org.scalatest.matchers.{Matcher, MatchResult}
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.Future
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpecLike

class RawDataValidatorSpec
    extends AsyncWordSpecLike
    with Matchers
    with MockitoSugar
    with EitherValues
    with ValidationMatchers {
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
  val databaseError = DatabaseError(ErrorCategory.Transient, ErrorName.CommunicationsLinkFailure, "oh no")

  trait ValidatorScope {
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

  "#validateEmptyCriteria" should {
    "return Valid if the data is not empty" in {
      RawDataValidator.validateEmptyCriteria(Map("field1" -> StringValue("value1"))).value.map(_ should beValid)
    }

    "return EmptyData if the data is empty" in {
      RawDataValidator.validateEmptyCriteria(Map()).value.map(_ should failWith(ErrorName.EmptyData))
    }
  }

  "#validateFieldExistence" should {
    "return Valid if all fields exist" in {
      val scope = new ValidatorScope {
        initFieldsForTable(tableName, Seq("field1", "field2"))
      }

      RawDataValidator
        .validateFieldExistence(tableName, Set("field1", "field2"), scope.connector)
        .value
        .map(_ should beValid)
    }

    "return NonExistingFields if some of the fields not exist" in {
      val scope = new ValidatorScope {
        initFieldsForTable(tableName, Seq("field1", "field2"))
      }

      RawDataValidator.validateFieldExistence(tableName, Set("field99", "field2"), scope.connector).value.map {
        result =>
          result should failWith(ErrorName.MissingFields)
          result should haveContext(Fields(List("field99")))
      }
    }

    "return the error if listFields fails" in {
      val scope = new ValidatorScope {
        when(connector.listFields(tableName)) thenReturn Future.successful(Left(databaseError))
      }

      RawDataValidator
        .validateFieldExistence(tableName, Set("field1", "field2"), scope.connector)
        .value
        .map(result => result.left.value shouldBe databaseError)
    }
  }

  "#validateTableExists" should {
    "return Valid if the table exists as table" in {
      val scope = new ValidatorScope {
        initTables()
      }

      RawDataValidator.validateTableExists(tableName, scope.connector).value.map(_ should beValid)
    }

    "return Valid if the table exists as view" in {
      val scope = new ValidatorScope {
        initTables()
      }

      RawDataValidator.validateTableExists(viewName, scope.connector).value.map(_ should beValid)
    }

    "return NonExistingTable if the table not exists neither as table or view" in {
      val scope = new ValidatorScope {
        initTables()
      }

      RawDataValidator
        .validateTableExists("unknown_table", scope.connector)
        .value
        .map(_ should failWith(ErrorName.NonExistingTable))
    }

    "return the error if listTables fails" in {
      val scope = new ValidatorScope {
        when(connector.listTables()) thenReturn
          Future.successful(Left(databaseError))
      }

      RawDataValidator
        .validateTableExists("unknown_table", scope.connector)
        .value
        .map(_.left.value shouldBe databaseError)
    }
  }

  "#validateTableExistsAndNotView" should {
    "return Valid if the table exists as table" in {
      val scope = new ValidatorScope {
        initTables()
      }

      RawDataValidator.validateTableExistsAndNotView(tableName, scope.connector).value.map(_ should beValid)
    }

    "return InvalidOperationOnView if the table exists as view" in {
      val scope = new ValidatorScope {
        initTables()
      }

      RawDataValidator
        .validateTableExistsAndNotView(viewName, scope.connector)
        .value
        .map(_ should failWith(ErrorName.InvalidOperationOnView))
    }

    "return NonExistingTable if the table not exists neither as table or view" in {
      val scope = new ValidatorScope {
        initTables()
      }

      RawDataValidator
        .validateTableExistsAndNotView("unknown_table", scope.connector)
        .value
        .map(_ should failWith(ErrorName.NonExistingTable))
    }

    "return the error if listTables fails" in {
      val scope = new ValidatorScope {
        when(connector.listTables()) thenReturn
          Future.successful(Left(databaseError))
      }

      RawDataValidator
        .validateTableExistsAndNotView("unknown_table", scope.connector)
        .value
        .map(_.left.value shouldBe databaseError)
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

    "return Valid if everything is correct" in {
      val scope = new ValidatorScope {
        initFieldsForTable(tableName, Seq("search_field11", "search_field12", "update_field11", "update_field12"))
        initIsOptimized(tableName, Seq("search_field11", "search_field12"), isOptimized = true)
      }

      RawDataValidator.validateUpdateFields(tableName, updateData, scope.connector).value.map(_ should beValid)
    }

    "return EmptyData if the update data is empty" in {
      val scope = new ValidatorScope {
        initFieldsForTable(tableName, Seq("search_field11", "search_field12", "update_field11", "update_field12"))
        initIsOptimized(tableName, Seq("search_field11", "search_field12"), isOptimized = true)
      }

      RawDataValidator
        .validateUpdateFields(tableName, Seq(), scope.connector)
        .value
        .map(_ should failWith(ErrorName.EmptyData))
    }

    "return NonExistingFields if not all keys of first UpdateDefinition exists as field" in {
      val nonExistingField = List("search_field11", "update_field12")
      val scope = new ValidatorScope {
        initFieldsForTable(tableName, nonExistingField)
        initIsOptimized(tableName, nonExistingField, isOptimized = true)
      }

      RawDataValidator.validateUpdateFields(tableName, updateData, scope.connector).value.map { result =>
        result should failWith(ErrorName.MissingFields)
        result should haveContext(Fields(nonExistingField))
      }
    }

    "return NoIndexOnFields if not all keys of search of first UpdateDefinition optimized as field" in {
      val scope = new ValidatorScope {
        initFieldsForTable(tableName, Seq("search_field11", "search_field12", "update_field11", "update_field12"))
        initIsOptimized(tableName, Seq("search_field11", "search_field12"), isOptimized = false)
      }

      RawDataValidator
        .validateUpdateFields(tableName, updateData, scope.connector)
        .value
        .map(_ should failWith(ErrorName.NoIndexOnFields))
    }

    "return the error if listFields fails" in {
      val scope = new ValidatorScope {
        when(connector.listFields(tableName)) thenReturn
          Future.successful(Left(databaseError))
      }

      RawDataValidator
        .validateUpdateFields(tableName, updateData, scope.connector)
        .value
        .map(_.left.value shouldBe databaseError)
    }
  }

  "#validateIndices" should {
    "return Valid if the fields are optimized" in {
      val scope = new ValidatorScope {
        initIsOptimized(tableName, Seq("field1", "field2"), isOptimized = true)
      }

      RawDataValidator.validateIndices(tableName, Set("field1", "field2"), scope.connector).value.map(_ should beValid)
    }

    "return NoIndexOnFields if the fields are not optimized" in {
      val scope = new ValidatorScope {
        initIsOptimized(tableName, Seq("field1", "field2"), isOptimized = false)
      }

      RawDataValidator
        .validateIndices(tableName, Set("field1", "field2"), scope.connector)
        .value
        .map(_ should failWith(ErrorName.NoIndexOnFields))
    }

    "return the error if listFields fails" in {
      val scope = new ValidatorScope {
        when(connector.isOptimized(tableName, Seq("field1", "field2"))) thenReturn
          Future.successful(Left(databaseError))
      }

      RawDataValidator
        .validateIndices(tableName, Set("field1", "field2"), scope.connector)
        .value
        .map(
          _.left.value shouldBe
            databaseError
        )
    }
  }

  "#validateFormat" should {
    val record1            = Map("k1" -> StringValue("r1_v"))
    val record2            = Map("k1" -> StringValue("r2_v"))
    val record3            = Map("k1" -> StringValue("r3_v"))
    val recordDifferentKey = Map("k1" -> StringValue("r4_v1"), "k2" -> StringValue("r4_v2"))

    "return TooManyRows if there are too many data" in {
      RawDataValidator
        .validateFormat(Seq(record1, record2, record3), 2)
        .value
        .map(_ should failWith(ErrorName.TooManyRows))
    }

    "return EmptyData if the data is empty" in {
      RawDataValidator.validateFormat(Seq(), 2).value.map(_ should failWith(ErrorName.EmptyData))
    }

    "return DifferentFields if the data is empty" in {
      RawDataValidator
        .validateFormat(Seq(record1, record2, recordDifferentKey), 3)
        .value
        .map(_ should failWith(ErrorName.DifferentFields))
    }

    "return Valid everything is ok" in {
      RawDataValidator.validateFormat(Seq(record1, record2, record3), 3).value.map(_ should beValid)
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

    "return TooManyRows if there are too many data" in {
      RawDataValidator
        .validateUpdateFormat(Seq(updateData1, updateData2, updateData3), 2)
        .value
        .map(_ should failWith(ErrorName.TooManyRows))
    }

    "return EmptyData if the data is empty" in {
      RawDataValidator.validateUpdateFormat(Seq(), 2).value.map(_ should failWith(ErrorName.EmptyData))
    }

    "return EmptyCriteria if there is an UpdateDefinition with empty criteria" in {
      val updateDataWithEmptySearch = UpdateDefinition(
        search = Map(),
        update = Map("k" -> StringValue("v"))
      )
      val data = Seq(updateData1, updateData2, updateDataWithEmptySearch)

      RawDataValidator.validateUpdateFormat(data, 3).value.map(_ should failWith(ErrorName.EmptyCriteria))
    }

    "return EmptyData if there is an UpdateDefinition with empty criteria" in {
      val updateDataWithEmptyUpdate = UpdateDefinition(
        search = Map("k" -> StringValue("v")),
        update = Map()
      )
      val data = Seq(updateData1, updateData2, updateDataWithEmptyUpdate)

      RawDataValidator.validateUpdateFormat(data, 3).value.map(_ should failWith(ErrorName.EmptyData))
    }

    "return DifferentFields if not all the search keys are the same" in {
      val updateDataWithDifferentSeach = UpdateDefinition(
        search = Map("field1" -> StringValue("value31"), "field2_different" -> StringValue("value32")),
        update = Map("field3" -> StringValue("value33"))
      )
      val data = Seq(updateData1, updateData2, updateDataWithDifferentSeach)

      RawDataValidator.validateUpdateFormat(data, 3).value.map(_ should failWith(ErrorName.DifferentFields))
    }

    "return DifferentFields if not all the update keys are the same" in {
      val updateDataWithDifferentSearch = UpdateDefinition(
        search = Map("field1"           -> StringValue("value31"), "field2" -> StringValue("value32")),
        update = Map("field3_different" -> StringValue("value33"))
      )
      val data = Seq(updateData1, updateData2, updateDataWithDifferentSearch)

      RawDataValidator.validateUpdateFormat(data, 3).value.map(_ should failWith(ErrorName.DifferentFields))
    }

    "return Valid if everything is ok" in {
      val data = Seq(updateData1, updateData2, updateData3)

      RawDataValidator.validateUpdateFormat(data, 3).value.map(_ should beValid)
    }
  }

}

trait ValidationMatchers {

  class ValidMatcher extends Matcher[Either[DatabaseError, Unit]] {
    override def apply(result: Either[DatabaseError, Unit]): MatchResult = MatchResult(
      result.isRight,
      s"Expected to pass validation, failed with $result",
      s"Passed validation"
    )
  }

  def beValid = new ValidMatcher

  class InvalidMatcher(reason: ErrorName) extends Matcher[Either[DatabaseError, Unit]] {
    override def apply(result: Either[DatabaseError, Unit]): MatchResult = MatchResult(
      result match {
        case Left(DatabaseError(ErrorCategory.Validation, `reason`, _, _, _)) => true
        case _                                                                => false
      },
      s"Expected to fail validation with $reason, got $result",
      s"Failed validation with $reason"
    )
  }

  def failWith(reason: ErrorName) = new InvalidMatcher(reason)

  class ValidationContextMatcher(context: Context) extends Matcher[Either[DatabaseError, Unit]] {
    override def apply(result: Either[DatabaseError, Unit]): MatchResult = MatchResult(
      result match {
        case Left(DatabaseError(ErrorCategory.Validation, _, _, _, Some(context))) => true
        case _                                                                     => false
      },
      s"Expected to fail validation with $context, got $result",
      s"Failed validation with $context"
    )
  }

  def haveContext(context: Context) = new ValidationContextMatcher(context)
}
