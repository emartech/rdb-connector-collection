package com.emarsys.rdb.connector.common.models

import cats.data.EitherT
import cats.instances.future._
import com.emarsys.rdb.connector.common.{ConnectorResponse, ConnectorResponseET}
import com.emarsys.rdb.connector.common.models.DataManipulation.{Criteria, Record, UpdateDefinition}
import com.emarsys.rdb.connector.common.models.Errors.ConnectorError

import scala.concurrent.{ExecutionContext, Future}

class DataManipulationValidator(validator: RawDataValidator) {
  private type DeferredValidation = () => ConnectorResponseET[ValidationResult]

  private val MaxRows = 1000

  def validateUpdateDefinition(tableName: String, updateData: Seq[UpdateDefinition], connector: Connector)(
      implicit ec: ExecutionContext
  ): ConnectorResponse[ValidationResult] = {
    runValidations(
      () => validator.validateUpdateFormat(updateData, MaxRows),
      () => validator.validateTableExistsAndNotView(tableName, connector),
      () => validator.validateUpdateFields(tableName, updateData, connector)
    ).value
  }

  def validateInsertData(tableName: String, dataToInsert: Seq[Record], connector: Connector)(
      implicit ec: ExecutionContext
  ): ConnectorResponse[ValidationResult] = {
    val validationResult = runValidations(
      () => validator.validateFormat(dataToInsert, MaxRows),
      () => validator.validateTableExistsAndNotView(tableName, connector),
      () => validator.validateFieldExistence(tableName, dataToInsert.head.keySet, connector)
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
      () => validator.validateFormat(criteria, MaxRows),
      () => validator.validateTableExistsAndNotView(tableName, connector),
      () => validator.validateFieldExistence(tableName, criteria.head.keySet, connector),
      () => validator.validateIndices(tableName, criteria.head.keySet, connector)
    ).value
  }

  def validateSearchCriteria(tableName: String, criteria: Criteria, connector: Connector)(
      implicit ec: ExecutionContext
  ): ConnectorResponse[ValidationResult] = {
    runValidations(
      () => validator.validateEmptyCriteria(criteria),
      () => validator.validateTableExists(tableName, connector),
      () => validator.validateFieldExistence(tableName, criteria.keySet, connector),
      () => validator.validateIndices(tableName, criteria.keySet, connector)
    ).value
  }

  private def runValidations(
      validations: DeferredValidation*
  )(implicit ec: ExecutionContext): ConnectorResponseET[ValidationResult] = {
    if (validations.isEmpty) {
      EitherT.rightT[Future, ConnectorError](ValidationResult.Valid)
    } else {
      // TODO: head
      val validation = validations.head.apply()

      validation flatMap {
        case ValidationResult.Valid => runValidations(validations.tail: _*)
        case validationResult       => EitherT.rightT[Future, ConnectorError](validationResult)
      }
    }
  }

}
