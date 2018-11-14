package com.emarsys.rdb.connector.common.models

import com.emarsys.rdb.connector.common.models.DataManipulation.{Criteria, Record, UpdateDefinition}
import com.emarsys.rdb.connector.common.models.Errors.ErrorWithMessage
import com.emarsys.rdb.connector.common.models.ValidateDataManipulation.ValidationResult

import scala.concurrent.{ExecutionContext, Future}

trait ValidateDataManipulation {
  private type DeferredValidation = () => Future[ValidationResult]

  val maxRows: Int

  def validateUpdateDefinition(tableName: String, updateData: Seq[UpdateDefinition], connector: Connector)(implicit executionContext: ExecutionContext): Future[ValidationResult] = {
    runValidations(Seq(
      () => validateUpdateFormat(updateData),
      () => validateTableIsNotAView(tableName, connector),
      () => validateUpdateFields(tableName, updateData, connector)
    ))
  }

  def validateInsertData(tableName: String, dataToInsert: Seq[Record], connector: Connector)(implicit executionContext: ExecutionContext): Future[ValidationResult] = {
    runValidations(Seq(
      () => validateFormat(dataToInsert),
      () => validateTableIsNotAView(tableName, connector),
      () => validateFieldExistence(tableName, dataToInsert.head.keySet, connector)
    )) map {
      case ValidationResult.EmptyData =>
        ValidationResult.Valid
      case failedValidationResult =>
        failedValidationResult
    }
  }

  def validateDeleteCriteria(tableName: String, criteria: Seq[Criteria], connector: Connector)(implicit executionContext: ExecutionContext): Future[ValidationResult] = {
    runValidations(Seq(
      () => validateFormat(criteria),
      () => validateTableIsNotAView(tableName, connector),
      () => validateFieldExistence(tableName, criteria.head.keySet, connector),
      () => validateIndices(tableName, criteria.head.keySet, connector)
    ))
  }

  def validateSearchCriteria(tableName: String, criteria: Criteria, connector: Connector)(implicit executionContext: ExecutionContext): Future[ValidationResult] = {
    runValidations(Seq(
      () => validateEmtpyCriteria(criteria),
      () => validateFieldExistence(tableName, criteria.keySet, connector),
      () => validateIndices(tableName, criteria.keySet, connector)
    ))
  }

  private def validateEmtpyCriteria(data: Criteria) = Future.successful[ValidationResult] {
    if (data.isEmpty) {
      ValidationResult.EmptyData
    } else {
      ValidationResult.Valid
    }
  }

  private def runValidations(validations: Seq[DeferredValidation])(implicit executionContext: ExecutionContext): Future[ValidationResult] = {
    if (validations.isEmpty) {
      Future.successful(ValidationResult.Valid)
    } else {
      val validation = validations.head.apply()

      validation flatMap {
        case ValidationResult.Valid =>
          runValidations(validations.tail)
        case failedValidationResult =>
          Future.successful(failedValidationResult)
      }
    }
  }

  private def validateFieldExistence(tableName: String, updateData: Seq[UpdateDefinition], connector: Connector)(implicit executionContext: ExecutionContext) : Future[ValidationResult] = {
    if(updateData.isEmpty){
      Future.successful(ValidationResult.EmptyData)
    } else {
      val keyFields = updateData.head.search.keySet ++ updateData.head.update.keySet
      validateFieldExistence(tableName, keyFields, connector)
    }
  }

  private def validateFieldExistence(tableName: String, keyFields: Set[String], connector: Connector)(implicit executionContext: ExecutionContext): Future[ValidationResult] = {
    connector.listFields(tableName).map {
      case Right(columns) =>
        val nonExistingFields = keyFields.map(_.toLowerCase).diff(columns.map(_.name.toLowerCase).toSet)
        if (nonExistingFields.isEmpty) {
          ValidationResult.Valid
        } else {
          ValidationResult.NonExistingFields(keyFields.filter(kf => nonExistingFields.contains(kf.toLowerCase)))
        }
      case Left(ErrorWithMessage(msg)) => ValidationResult.ValidationFailed(msg)
      case _ => ValidationResult.NonExistingTable
    }
  }

  private def validateTableIsNotAView(tableName: String, connector: Connector)(implicit executionContext: ExecutionContext): Future[ValidationResult] = {
    connector.listTables().map {
      case Right(tableModels) =>
        val tableNameIsAView = tableModels.exists { tableModel =>
          tableModel.name == tableName && tableModel.isView
        }
        if (tableNameIsAView)
          ValidationResult.InvalidOperationOnView
        else
          ValidationResult.Valid

      case Left(ErrorWithMessage(msg)) => ValidationResult.ValidationFailed(msg)
      case Left(ex) => ValidationResult.ValidationFailed(s"Something went wrong: $ex")
    }
  }

  private def validateUpdateFields(tableName: String, updateData: Seq[UpdateDefinition], connector: Connector)(implicit executionContext: ExecutionContext) : Future[ValidationResult] = {
    validateFieldExistence(tableName, updateData, connector) flatMap {
      case ValidationResult.Valid =>
        validateIndices(tableName, updateData.head.search.keySet, connector)
      case failedValidationResult =>
        Future.successful(failedValidationResult)
    }
  }

  private def validateIndices(tableName: String, keyFields: Set[String], connector: Connector)(implicit executionContext: ExecutionContext): Future[ValidationResult] = {
    connector.isOptimized(tableName, keyFields.toList).map {
      case Right(true) => ValidationResult.Valid
      case Right(false) => ValidationResult.NoIndexOnFields
      case Left(ErrorWithMessage(msg)) => ValidationResult.ValidationFailed(msg)
      case Left(ex) => ValidationResult.ValidationFailed(s"Something went wrong: $ex")
    }
  }

  private def validateFormat(data: Seq[Record]) = Future.successful[ValidationResult] {
    if (data.size > maxRows) {
      ValidationResult.TooManyRows
    } else if (data.isEmpty) {
      ValidationResult.EmptyData
    } else if (!areAllKeysTheSame(data)) {
      ValidationResult.DifferentFields
    } else {
      ValidationResult.Valid
    }
  }

  private def areAllKeysTheSame(dataToInsert: Seq[Record]): Boolean = {
    val firstRecordsKeySet = dataToInsert.head.keySet
    dataToInsert.forall(_.keySet == firstRecordsKeySet)
  }

  private def validateUpdateFormat(updateData: Seq[UpdateDefinition]) = Future.successful {
    if (updateData.size > maxRows) {
      ValidationResult.TooManyRows
    } else if (updateData.isEmpty) {
      ValidationResult.EmptyData
    } else if (hasEmptyCriteria(updateData)) {
      ValidationResult.EmptyCriteria
    } else if (hasEmptyData(updateData)) {
      ValidationResult.EmptyData
    } else if (!areAllCriteriaFieldsTheSame(updateData)) {
      ValidationResult.DifferentFields
    } else if (!areAllUpdateFieldsTheSame(updateData)) {
      ValidationResult.DifferentFields
    } else {
      ValidationResult.Valid
    }
  }

  private def areAllCriteriaFieldsTheSame(data: Seq[UpdateDefinition]) = {
    val firstRecordCriteriaKeySet = data.head.search.keySet
    data.forall(_.search.keySet == firstRecordCriteriaKeySet)
  }

  private def areAllUpdateFieldsTheSame(data: Seq[UpdateDefinition]) = {
    val firstRecordUpdateKeySet = data.head.update.keySet
    data.forall(_.update.keySet == firstRecordUpdateKeySet)
  }

  private def hasEmptyCriteria(updateData: Seq[UpdateDefinition]) = updateData.exists(_.search.isEmpty)

  private def hasEmptyData(updateData: Seq[UpdateDefinition]) = updateData.exists(_.update.isEmpty)
}

object ValidateDataManipulation extends ValidateDataManipulation {

  val maxRows = 1000

  sealed trait ValidationResult

  object ValidationResult {
    case object Valid extends ValidationResult
    case object DifferentFields extends ValidationResult
    case object EmptyData extends ValidationResult
    case object TooManyRows extends ValidationResult
    case object NonExistingTable extends ValidationResult
    case class NonExistingFields(fields: Set[String]) extends ValidationResult

    case object NoIndexOnFields extends ValidationResult

    case object EmptyCriteria extends ValidationResult

    case object InvalidOperationOnView extends ValidationResult

    case class ValidationFailed(message: String) extends ValidationResult
  }

}
