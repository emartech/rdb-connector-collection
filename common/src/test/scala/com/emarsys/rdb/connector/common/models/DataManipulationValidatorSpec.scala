package com.emarsys.rdb.connector.common.models

import cats.data.EitherT
import cats.instances.future._
import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper.StringValue
import com.emarsys.rdb.connector.common.models.DataManipulation.UpdateDefinition
import com.emarsys.rdb.connector.common.models.Errors.{ConnectorError, DatabaseError, ErrorCategory, ErrorName}
import com.emarsys.rdb.connector.common.models.TableSchemaDescriptors.TableModel
import com.emarsys.rdb.connector.common.{ConnectorResponse, ConnectorResponseET}
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{EitherValues, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class DataManipulationValidatorSpec extends WordSpecLike with Matchers with MockitoSugar with EitherValues {
  implicit val executionContext: ExecutionContext = ExecutionContext.global

  val tableName       = "tableName"
  val viewName        = "viewName"
  val availableTables = Future.successful(Right(Seq(TableModel(tableName, false), TableModel(viewName, true))))
  val optimizedField  = Future.successful(Right(true))

  val failure = DatabaseError(ErrorCategory.Transient, ErrorName.CommunicationsLinkFailure, "oh no")

  class ValidatorScope {
    val connector = new Connector {
      implicit val executionContext: ExecutionContext = null
      def close(): Future[Unit]                       = null
    }
    val dataValidator = mock[RawDataValidator]
    val validator     = new DataManipulationValidator(dataValidator)
  }

  private def await(f: ConnectorResponse[ValidationResult]): Either[ConnectorError, ValidationResult] = {
    Await.result(f, 3.seconds)
  }

  private def toResponse(validationResult: ValidationResult): ConnectorResponseET[ValidationResult] = {
    EitherT.rightT[Future, ConnectorError](validationResult)
  }

  private def toResponse(error: ConnectorError): ConnectorResponseET[ValidationResult] = {
    EitherT.leftT[Future, ValidationResult](error)
  }

  "#validateUpdateData" should {

    val updateData = Seq(UpdateDefinition(Map("a" -> StringValue("1")), Map("b" -> StringValue("2"))))

    "call validateUpdateFormat, validateTableExistsAndNotView and validateUpdateFields" in new ValidatorScope {
      when(dataValidator.validateUpdateFormat(updateData, 1000)(executionContext)) thenReturn
        toResponse(ValidationResult.Valid)
      when(dataValidator.validateTableExistsAndNotView(tableName, connector)(executionContext)) thenReturn
        toResponse(ValidationResult.Valid)
      when(dataValidator.validateUpdateFields(tableName, updateData, connector)(executionContext)) thenReturn
        toResponse(ValidationResult.Valid)

      await(validator.validateUpdateDefinition(tableName, updateData, connector)).right.value shouldBe
        ValidationResult.Valid

      verify(dataValidator).validateUpdateFormat(updateData, 1000)
      verify(dataValidator).validateTableExistsAndNotView(tableName, connector)
      verify(dataValidator).validateUpdateFields(tableName, updateData, connector)
    }

    "stop validation if one of the validation return invalid result" in new ValidatorScope {
      when(dataValidator.validateUpdateFormat(updateData, 1000)(executionContext)) thenReturn
        toResponse(ValidationResult.Valid)
      when(dataValidator.validateTableExistsAndNotView(tableName, connector)(executionContext)) thenReturn
        toResponse(ValidationResult.NoIndexOnFields)

      await(validator.validateUpdateDefinition(tableName, updateData, connector)).right.value shouldBe
        ValidationResult.NoIndexOnFields

      verify(dataValidator).validateUpdateFormat(updateData, 1000)
      verify(dataValidator).validateTableExistsAndNotView(tableName, connector)
      verify(dataValidator, never()).validateUpdateFields(tableName, updateData, connector)
    }

    "fail if one of the validation fails" in new ValidatorScope {
      when(dataValidator.validateUpdateFormat(updateData, 1000)(executionContext)) thenReturn
        toResponse(ValidationResult.Valid)
      when(dataValidator.validateTableExistsAndNotView(tableName, connector)(executionContext)) thenReturn
        toResponse(failure)

      await(validator.validateUpdateDefinition(tableName, updateData, connector)).left.value shouldBe
        failure

      verify(dataValidator).validateUpdateFormat(updateData, 1000)
      verify(dataValidator).validateTableExistsAndNotView(tableName, connector)
      verify(dataValidator, never()).validateUpdateFields(tableName, updateData, connector)
    }
  }

  "#validateInsertData" should {

    val dataToInsert = Seq(
      Map("a" -> StringValue("1"), "b" -> StringValue("2")),
      Map("c" -> StringValue("3"))
    )

    "call validateFormat, validateTableExistsAndNotView and validateFieldExistence" in new ValidatorScope {
      when(dataValidator.validateFormat(dataToInsert, 1000)(executionContext)) thenReturn
        toResponse(ValidationResult.Valid)
      when(dataValidator.validateTableExistsAndNotView(tableName, connector)(executionContext)) thenReturn
        toResponse(ValidationResult.Valid)
      when(dataValidator.validateFieldExistence(tableName, Set("a", "b"), connector)(executionContext)) thenReturn
        toResponse(ValidationResult.Valid)

      await(validator.validateInsertData(tableName, dataToInsert, connector)).right.value shouldBe
        ValidationResult.Valid

      verify(dataValidator).validateFormat(dataToInsert, 1000)
      verify(dataValidator).validateTableExistsAndNotView(tableName, connector)
      verify(dataValidator).validateFieldExistence(tableName, Set("a", "b"), connector)
    }

    "stop validation if one of the validation return invalid result" in new ValidatorScope {
      when(dataValidator.validateFormat(dataToInsert, 1000)(executionContext)) thenReturn
        toResponse(ValidationResult.Valid)
      when(dataValidator.validateTableExistsAndNotView(tableName, connector)(executionContext)) thenReturn
        toResponse(ValidationResult.NoIndexOnFields)

      await(validator.validateInsertData(tableName, dataToInsert, connector)).right.value shouldBe
        ValidationResult.NoIndexOnFields

      verify(dataValidator).validateFormat(dataToInsert, 1000)
      verify(dataValidator).validateTableExistsAndNotView(tableName, connector)
      verify(dataValidator, never()).validateFieldExistence(tableName, Set("a", "b"), connector)
    }

    "fail if one of the validation fails" in new ValidatorScope {
      when(dataValidator.validateFormat(dataToInsert, 1000)(executionContext)) thenReturn
        toResponse(ValidationResult.Valid)
      when(dataValidator.validateTableExistsAndNotView(tableName, connector)(executionContext)) thenReturn
        toResponse(failure)

      await(validator.validateInsertData(tableName, dataToInsert, connector)).left.value shouldBe
        failure

      verify(dataValidator).validateFormat(dataToInsert, 1000)
      verify(dataValidator).validateTableExistsAndNotView(tableName, connector)
      verify(dataValidator, never()).validateFieldExistence(tableName, Set("a", "b"), connector)
    }

    "return Valid if one of the validation fails with EmptyData" in new ValidatorScope {
      when(dataValidator.validateFormat(dataToInsert, 1000)(executionContext)) thenReturn
        toResponse(ValidationResult.Valid)
      when(dataValidator.validateTableExistsAndNotView(tableName, connector)(executionContext)) thenReturn
        toResponse(ValidationResult.EmptyData)

      await(validator.validateInsertData(tableName, dataToInsert, connector)).right.value shouldBe
        ValidationResult.Valid

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
      when(dataValidator.validateFormat(criteria, 1000)(executionContext)) thenReturn
        toResponse(ValidationResult.Valid)
      when(dataValidator.validateTableExistsAndNotView(tableName, connector)(executionContext)) thenReturn
        toResponse(ValidationResult.Valid)
      when(dataValidator.validateFieldExistence(tableName, Set("a", "b"), connector)(executionContext)) thenReturn
        toResponse(ValidationResult.Valid)
      when(dataValidator.validateIndices(tableName, Set("a", "b"), connector)(executionContext)) thenReturn
        toResponse(ValidationResult.Valid)

      await(validator.validateDeleteCriteria(tableName, criteria, connector)).right.value shouldBe
        ValidationResult.Valid

      verify(dataValidator).validateFormat(criteria, 1000)
      verify(dataValidator).validateTableExistsAndNotView(tableName, connector)
      verify(dataValidator).validateFieldExistence(tableName, Set("a", "b"), connector)
      verify(dataValidator).validateIndices(tableName, Set("a", "b"), connector)
    }

    "stop validation if one of the validation return invalid result" in new ValidatorScope {
      when(dataValidator.validateFormat(criteria, 1000)(executionContext)) thenReturn
        toResponse(ValidationResult.Valid)
      when(dataValidator.validateTableExistsAndNotView(tableName, connector)(executionContext)) thenReturn
        toResponse(ValidationResult.NoIndexOnFields)

      await(validator.validateDeleteCriteria(tableName, criteria, connector)).right.value shouldBe
        ValidationResult.NoIndexOnFields

      verify(dataValidator).validateFormat(criteria, 1000)
      verify(dataValidator).validateTableExistsAndNotView(tableName, connector)
      verify(dataValidator, never()).validateFieldExistence(tableName, Set("a", "b"), connector)
    }

    "fail if one of the validation fails" in new ValidatorScope {
      when(dataValidator.validateFormat(criteria, 1000)(executionContext)) thenReturn
        toResponse(ValidationResult.Valid)
      when(dataValidator.validateTableExistsAndNotView(tableName, connector)(executionContext)) thenReturn
        toResponse(failure)

      await(validator.validateDeleteCriteria(tableName, criteria, connector)).left.value shouldBe
        failure

      verify(dataValidator).validateFormat(criteria, 1000)
      verify(dataValidator).validateTableExistsAndNotView(tableName, connector)
      verify(dataValidator, never()).validateFieldExistence(tableName, Set("a", "b"), connector)
    }
  }

  "#validateSearchCriteria" should {

    val criteria = Map("a" -> StringValue("1"), "b" -> StringValue("2"))

    "call validateEmptyCriteria, validateTableExistsAndNotView, validateFieldExistence and validateIndices" in new ValidatorScope {
      when(dataValidator.validateEmptyCriteria(criteria)(executionContext)) thenReturn
        toResponse(ValidationResult.Valid)
      when(dataValidator.validateTableExists(tableName, connector)(executionContext)) thenReturn
        toResponse(ValidationResult.Valid)
      when(dataValidator.validateFieldExistence(tableName, Set("a", "b"), connector)(executionContext)) thenReturn
        toResponse(ValidationResult.Valid)
      when(dataValidator.validateIndices(tableName, Set("a", "b"), connector)(executionContext)) thenReturn
        toResponse(ValidationResult.Valid)

      await(validator.validateSearchCriteria(tableName, criteria, connector)).right.value shouldBe
        ValidationResult.Valid

      verify(dataValidator).validateEmptyCriteria(criteria)
      verify(dataValidator).validateTableExists(tableName, connector)
      verify(dataValidator).validateFieldExistence(tableName, Set("a", "b"), connector)
      verify(dataValidator).validateIndices(tableName, Set("a", "b"), connector)
    }

    "stop validation if one of the validation return invalid result" in new ValidatorScope {
      when(dataValidator.validateEmptyCriteria(criteria)(executionContext)) thenReturn
        toResponse(ValidationResult.Valid)
      when(dataValidator.validateTableExists(tableName, connector)(executionContext)) thenReturn
        toResponse(ValidationResult.NoIndexOnFields)

      await(validator.validateSearchCriteria(tableName, criteria, connector)).right.value shouldBe
        ValidationResult.NoIndexOnFields

      verify(dataValidator).validateEmptyCriteria(criteria)
      verify(dataValidator).validateTableExists(tableName, connector)
      verify(dataValidator, never()).validateFieldExistence(tableName, Set("a", "b"), connector)
    }

    "fail if one of the validation fails" in new ValidatorScope {
      when(dataValidator.validateEmptyCriteria(criteria)(executionContext)) thenReturn
        toResponse(ValidationResult.Valid)
      when(dataValidator.validateTableExists(tableName, connector)(executionContext)) thenReturn
        toResponse(failure)

      await(validator.validateSearchCriteria(tableName, criteria, connector)).left.value shouldBe
        failure

      verify(dataValidator).validateEmptyCriteria(criteria)
      verify(dataValidator).validateTableExists(tableName, connector)
      verify(dataValidator, never()).validateFieldExistence(tableName, Set("a", "b"), connector)
    }
  }

}
