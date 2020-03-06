package com.emarsys.rdb.connector.common.models

import com.emarsys.rdb.connector.common.{ConnectorResponse, ConnectorResponseET}
import com.emarsys.rdb.connector.common.models.DataManipulation.{Criteria, Record, UpdateDefinition}
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName}

import scala.concurrent.ExecutionContext

class DataManipulationValidator(validator: RawDataValidator) {
  import cats.instances.future._
  import cats.instances.list._
  import cats.syntax.foldable._

  private type DeferredValidation = () => ConnectorResponseET[Unit]

  private val MaxRows = 1000

  def validateUpdateDefinition(tableName: String, updateData: Seq[UpdateDefinition], connector: Connector)(
      implicit ec: ExecutionContext
  ): ConnectorResponse[Unit] = {
    runValidations(
      () => validator.validateUpdateFormat(updateData, MaxRows),
      () => validator.validateTableExistsAndNotView(tableName, connector),
      () => validator.validateUpdateFields(tableName, updateData, connector)
    ).value
  }

  def validateInsertData(tableName: String, dataToInsert: Seq[Record], connector: Connector)(
      implicit ec: ExecutionContext
  ): ConnectorResponse[Unit] = {
    val validationResult = runValidations(
      () => validator.validateFormat(dataToInsert, MaxRows),
      () => validator.validateTableExistsAndNotView(tableName, connector),
      () => validator.validateFieldExistence(tableName, dataToInsert.head.keySet, connector)
    ) recover {
      case DatabaseError(ErrorCategory.Validation, ErrorName.EmptyData, _, _, _) => ()
    }

    validationResult.value
  }

  def validateDeleteCriteria(tableName: String, criteria: Seq[Criteria], connector: Connector)(
      implicit ec: ExecutionContext
  ): ConnectorResponse[Unit] = {
    runValidations(
      () => validator.validateFormat(criteria, MaxRows),
      () => validator.validateTableExistsAndNotView(tableName, connector),
      () => validator.validateFieldExistence(tableName, criteria.head.keySet, connector),
      () => validator.validateIndices(tableName, criteria.head.keySet, connector)
    ).value
  }

  def validateSearchCriteria(tableName: String, criteria: Criteria, connector: Connector)(
      implicit ec: ExecutionContext
  ): ConnectorResponse[Unit] = {
    runValidations(
      () => validator.validateEmptyCriteria(criteria),
      () => validator.validateTableExists(tableName, connector),
      () => validator.validateFieldExistence(tableName, criteria.keySet, connector),
      () => validator.validateIndices(tableName, criteria.keySet, connector)
    ).value
  }

  private def runValidations(validations: DeferredValidation*)(implicit ec: ExecutionContext) = {
    validations.toList.foldM[ConnectorResponseET, Unit](())((_, nextValidation) => nextValidation())
  }

}
