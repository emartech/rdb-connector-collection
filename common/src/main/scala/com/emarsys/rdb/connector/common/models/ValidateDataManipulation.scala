package com.emarsys.rdb.connector.common.models

import cats.data.EitherT
import cats.instances.future._
import com.emarsys.rdb.connector.common.{ConnectorResponse, ConnectorResponseET}
import com.emarsys.rdb.connector.common.models.DataManipulation.{Criteria, Record, UpdateDefinition}
import com.emarsys.rdb.connector.common.models.Errors.ConnectorError

import scala.concurrent.{ExecutionContext, Future}

trait ValidateDataManipulation {
  private type DeferredValidation = () => ConnectorResponseET[ValidationResult]

  val maxRows: Int

  def validateUpdateDefinition(tableName: String, updateData: Seq[UpdateDefinition], connector: Connector)(
      implicit ec: ExecutionContext
  ): ConnectorResponse[ValidationResult] = {
    runValidations(
      () => DataValidator.validateUpdateFormat(updateData, maxRows),
      () => DataValidator.validateTableExistsAndNotView(tableName, connector),
      () => DataValidator.validateUpdateFields(tableName, updateData, connector)
    ).value
  }

  def validateInsertData(tableName: String, dataToInsert: Seq[Record], connector: Connector)(
      implicit ec: ExecutionContext
  ): ConnectorResponse[ValidationResult] = {
    val validationResult = runValidations(
      () => DataValidator.validateFormat(dataToInsert, maxRows),
      () => DataValidator.validateTableExistsAndNotView(tableName, connector),
      () => DataValidator.validateFieldExistence(tableName, dataToInsert.head.keySet, connector)
    ) map {
      case ValidationResult.EmptyData => ValidationResult.Valid
      case otherValidationResult      => otherValidationResult
    }

    validationResult.value
  }

  def validateDeleteCriteria(tableName: String, criteria: Seq[Criteria], connector: Connector)(
      implicit ec: ExecutionContext
  ): ConnectorResponse[ValidationResult] = {
    runValidations(
      () => DataValidator.validateFormat(criteria, maxRows),
      () => DataValidator.validateTableExistsAndNotView(tableName, connector),
      () => DataValidator.validateFieldExistence(tableName, criteria.head.keySet, connector),
      () => DataValidator.validateIndices(tableName, criteria.head.keySet, connector)
    ).value
  }

  def validateSearchCriteria(tableName: String, criteria: Criteria, connector: Connector)(
      implicit ec: ExecutionContext
  ): ConnectorResponse[ValidationResult] = {
    runValidations(
      () => DataValidator.validateEmptyCriteria(criteria),
      () => DataValidator.validateTableExists(tableName, connector),
      () => DataValidator.validateFieldExistence(tableName, criteria.keySet, connector),
      () => DataValidator.validateIndices(tableName, criteria.keySet, connector)
    ).value
  }

  private def runValidations(
      validations: DeferredValidation*
  )(implicit ec: ExecutionContext): ConnectorResponseET[ValidationResult] = {
    if (validations.isEmpty) {
      EitherT.rightT[Future, ConnectorError](ValidationResult.Valid)
    } else {
      val validation = validations.head.apply()

      validation flatMap {
        case ValidationResult.Valid => runValidations(validations.tail: _*)
        case validationResult       => EitherT.rightT[Future, ConnectorError](validationResult)
      }
    }
  }

}

object ValidateDataManipulation extends ValidateDataManipulation {

  val maxRows = 1000

}
