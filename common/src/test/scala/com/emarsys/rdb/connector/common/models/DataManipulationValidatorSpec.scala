package com.emarsys.rdb.connector.common.models

import cats.data.EitherT
import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper.StringValue
import com.emarsys.rdb.connector.common.models.DataManipulation.UpdateDefinition
import com.emarsys.rdb.connector.common.models.Errors.ErrorName.NoIndexOnFields
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName}
import com.emarsys.rdb.connector.common.models.TableSchemaDescriptors.TableModel
import org.mockito.Mockito._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{EitherValues, Matchers, WordSpecLike}

import scala.concurrent.{ExecutionContext, Future}

class DataManipulationValidatorSpec
    extends WordSpecLike
    with Matchers
    with MockitoSugar
    with EitherValues
    with ScalaFutures {
  import cats.instances.future._

  implicit val executionContext: ExecutionContext = ExecutionContext.global

  val tableName       = "tableName"
  val viewName        = "viewName"
  val availableTables = Future.successful(Right(Seq(TableModel(tableName, false), TableModel(viewName, true))))
  val optimizedField  = Future.successful(Right(true))
  val failure         = DatabaseError(ErrorCategory.Transient, ErrorName.CommunicationsLinkFailure, "oh no")
  val validationError = DatabaseError.validation(NoIndexOnFields)
  val valid           = Right(())

  val validResult: EitherT[Future, DatabaseError, Unit]   = EitherT.rightT(())
  val invalidResult: EitherT[Future, DatabaseError, Unit] = EitherT.leftT(validationError)
  val failedResult: EitherT[Future, DatabaseError, Unit]  = EitherT.leftT(failure)
  val emptyData: EitherT[Future, DatabaseError, Unit] =
    EitherT.leftT(DatabaseError.validation(ErrorName.EmptyData))

  class ValidatorScope {
    val connector     = mock[Connector]
    val dataValidator = mock[RawDataValidator]

    val validator = new DataManipulationValidator(dataValidator)
  }

  "#validateUpdateData" should {

    val data = Seq(UpdateDefinition(Map("a" -> StringValue("1")), Map("b" -> StringValue("2"))))

    "call validateUpdateFormat, validateTableExistsAndNotView and validateUpdateFields" in new ValidatorScope {
      when(dataValidator.validateUpdateFormat(data, 1000)(executionContext)) thenReturn validResult
      when(dataValidator.validateTableExistsAndNotView(tableName, connector)(executionContext)) thenReturn validResult
      when(dataValidator.validateUpdateFields(tableName, data, connector)(executionContext)) thenReturn validResult

      validator.validateUpdateDefinition(tableName, data, connector).futureValue shouldBe valid

      verify(dataValidator).validateUpdateFormat(data, 1000)
      verify(dataValidator).validateTableExistsAndNotView(tableName, connector)
      verify(dataValidator).validateUpdateFields(tableName, data, connector)
    }

    "stop validation if one of the validation return invalid result" in new ValidatorScope {
      when(dataValidator.validateUpdateFormat(data, 1000)(executionContext)) thenReturn validResult
      when(dataValidator.validateTableExistsAndNotView(tableName, connector)(executionContext)) thenReturn invalidResult

      validator.validateUpdateDefinition(tableName, data, connector).futureValue.left.value shouldBe validationError

      verify(dataValidator).validateUpdateFormat(data, 1000)
      verify(dataValidator).validateTableExistsAndNotView(tableName, connector)
      verify(dataValidator, never()).validateUpdateFields(tableName, data, connector)
    }

    "fail if one of the validation fails" in new ValidatorScope {
      when(dataValidator.validateUpdateFormat(data, 1000)(executionContext)) thenReturn validResult
      when(dataValidator.validateTableExistsAndNotView(tableName, connector)(executionContext)) thenReturn failedResult

      validator.validateUpdateDefinition(tableName, data, connector).futureValue.left.value shouldBe failure

      verify(dataValidator).validateUpdateFormat(data, 1000)
      verify(dataValidator).validateTableExistsAndNotView(tableName, connector)
      verify(dataValidator, never()).validateUpdateFields(tableName, data, connector)
    }
  }

  "#validateInsertData" should {

    val dataToInsert = Seq(
      Map("a" -> StringValue("1"), "b" -> StringValue("2")),
      Map("c" -> StringValue("3"))
    )

    "call validateFormat, validateTableExistsAndNotView and validateFieldExistence" in new ValidatorScope {
      when(dataValidator.validateFormat(dataToInsert, 1000)(executionContext)) thenReturn validResult
      when(dataValidator.validateTableExistsAndNotView(tableName, connector)(executionContext)) thenReturn validResult
      when(dataValidator.validateFieldExistence(tableName, Set("a", "b"), connector)(executionContext)) thenReturn validResult

      validator.validateInsertData(tableName, dataToInsert, connector).futureValue shouldBe valid

      verify(dataValidator).validateFormat(dataToInsert, 1000)
      verify(dataValidator).validateTableExistsAndNotView(tableName, connector)
      verify(dataValidator).validateFieldExistence(tableName, Set("a", "b"), connector)
    }

    "stop validation if one of the validation return invalid result" in new ValidatorScope {
      when(dataValidator.validateFormat(dataToInsert, 1000)(executionContext)) thenReturn validResult
      when(dataValidator.validateTableExistsAndNotView(tableName, connector)(executionContext)) thenReturn invalidResult

      validator.validateInsertData(tableName, dataToInsert, connector).futureValue.left.value shouldBe validationError

      verify(dataValidator).validateFormat(dataToInsert, 1000)
      verify(dataValidator).validateTableExistsAndNotView(tableName, connector)
      verify(dataValidator, never()).validateFieldExistence(tableName, Set("a", "b"), connector)
    }

    "fail if one of the validation fails" in new ValidatorScope {
      when(dataValidator.validateFormat(dataToInsert, 1000)(executionContext)) thenReturn validResult
      when(dataValidator.validateTableExistsAndNotView(tableName, connector)(executionContext)) thenReturn failedResult

      validator.validateInsertData(tableName, dataToInsert, connector).futureValue.left.value shouldBe failure

      verify(dataValidator).validateFormat(dataToInsert, 1000)
      verify(dataValidator).validateTableExistsAndNotView(tableName, connector)
      verify(dataValidator, never()).validateFieldExistence(tableName, Set("a", "b"), connector)
    }

    "return valid if one of the validation fails with EmptyData" in new ValidatorScope {
      when(dataValidator.validateFormat(dataToInsert, 1000)(executionContext)) thenReturn validResult
      when(dataValidator.validateTableExistsAndNotView(tableName, connector)(executionContext)) thenReturn emptyData

      validator.validateInsertData(tableName, dataToInsert, connector).futureValue shouldBe valid

      verify(dataValidator).validateFormat(dataToInsert, 1000)
      verify(dataValidator).validateTableExistsAndNotView(tableName, connector)
      verify(dataValidator, never()).validateFieldExistence(tableName, Set("a", "b"), connector)
    }
  }

  "#validateDeleteCriteria" should {

    val criteria = Seq(
      Map("a" -> StringValue("1"), "b" -> StringValue("2")),
      Map("c" -> StringValue("3"))
    )

    "call validateFormat, validateTableExistsAndNotView, validateFieldExistence and validateIndices" in new ValidatorScope {
      when(dataValidator.validateFormat(criteria, 1000)(executionContext)) thenReturn validResult
      when(dataValidator.validateTableExistsAndNotView(tableName, connector)(executionContext)) thenReturn validResult
      when(dataValidator.validateFieldExistence(tableName, Set("a", "b"), connector)(executionContext)) thenReturn validResult
      when(dataValidator.validateIndices(tableName, Set("a", "b"), connector)(executionContext)) thenReturn validResult

      validator.validateDeleteCriteria(tableName, criteria, connector).futureValue shouldBe valid

      verify(dataValidator).validateFormat(criteria, 1000)
      verify(dataValidator).validateTableExistsAndNotView(tableName, connector)
      verify(dataValidator).validateFieldExistence(tableName, Set("a", "b"), connector)
      verify(dataValidator).validateIndices(tableName, Set("a", "b"), connector)
    }

    "stop validation if one of the validation return invalid result" in new ValidatorScope {
      when(dataValidator.validateFormat(criteria, 1000)(executionContext)) thenReturn validResult
      when(dataValidator.validateTableExistsAndNotView(tableName, connector)(executionContext)) thenReturn invalidResult

      validator.validateDeleteCriteria(tableName, criteria, connector).futureValue.left.value shouldBe validationError

      verify(dataValidator).validateFormat(criteria, 1000)
      verify(dataValidator).validateTableExistsAndNotView(tableName, connector)
      verify(dataValidator, never()).validateFieldExistence(tableName, Set("a", "b"), connector)
    }

    "fail if one of the validation fails" in new ValidatorScope {
      when(dataValidator.validateFormat(criteria, 1000)(executionContext)) thenReturn validResult
      when(dataValidator.validateTableExistsAndNotView(tableName, connector)(executionContext)) thenReturn failedResult

      validator.validateDeleteCriteria(tableName, criteria, connector).futureValue.left.value shouldBe failure

      verify(dataValidator).validateFormat(criteria, 1000)
      verify(dataValidator).validateTableExistsAndNotView(tableName, connector)
      verify(dataValidator, never()).validateFieldExistence(tableName, Set("a", "b"), connector)
    }
  }

  "#validateSearchCriteria" should {

    val criteria = Map("a" -> StringValue("1"), "b" -> StringValue("2"))

    "call validateEmptyCriteria, validateTableExistsAndNotView, validateFieldExistence and validateIndices" in new ValidatorScope {
      when(dataValidator.validateEmptyCriteria(criteria)(executionContext)) thenReturn validResult
      when(dataValidator.validateTableExists(tableName, connector)(executionContext)) thenReturn validResult
      when(dataValidator.validateFieldExistence(tableName, Set("a", "b"), connector)(executionContext)) thenReturn validResult
      when(dataValidator.validateIndices(tableName, Set("a", "b"), connector)(executionContext)) thenReturn validResult

      validator.validateSearchCriteria(tableName, criteria, connector).futureValue shouldBe valid

      verify(dataValidator).validateEmptyCriteria(criteria)
      verify(dataValidator).validateTableExists(tableName, connector)
      verify(dataValidator).validateFieldExistence(tableName, Set("a", "b"), connector)
      verify(dataValidator).validateIndices(tableName, Set("a", "b"), connector)
    }

    "stop validation if one of the validation return invalid result" in new ValidatorScope {
      when(dataValidator.validateEmptyCriteria(criteria)(executionContext)) thenReturn validResult
      when(dataValidator.validateTableExists(tableName, connector)(executionContext)) thenReturn invalidResult

      validator.validateSearchCriteria(tableName, criteria, connector).futureValue.left.value shouldBe validationError

      verify(dataValidator).validateEmptyCriteria(criteria)
      verify(dataValidator).validateTableExists(tableName, connector)
      verify(dataValidator, never()).validateFieldExistence(tableName, Set("a", "b"), connector)
    }

    "fail if one of the validation fails" in new ValidatorScope {
      when(dataValidator.validateEmptyCriteria(criteria)(executionContext)) thenReturn validResult
      when(dataValidator.validateTableExists(tableName, connector)(executionContext)) thenReturn failedResult

      validator.validateSearchCriteria(tableName, criteria, connector).futureValue.left.value shouldBe failure

      verify(dataValidator).validateEmptyCriteria(criteria)
      verify(dataValidator).validateTableExists(tableName, connector)
      verify(dataValidator, never()).validateFieldExistence(tableName, Set("a", "b"), connector)
    }
  }

}
