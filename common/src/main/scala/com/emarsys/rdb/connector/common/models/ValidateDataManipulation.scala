package com.emarsys.rdb.connector.common.models

import cats.data.EitherT
import cats.instances.future._
import com.emarsys.rdb.connector.common.{ConnectorResponse, ConnectorResponseET}
import com.emarsys.rdb.connector.common.models.DataManipulation.{Criteria, Record, UpdateDefinition}
import com.emarsys.rdb.connector.common.models.Errors.ConnectorError
import com.emarsys.rdb.connector.common.models.ValidateDataManipulation.ValidationResult
import com.emarsys.rdb.connector.common.models.ValidateDataManipulation.ValidationResult._

import scala.concurrent.{ExecutionContext, Future}

trait ValidateDataManipulation {
  private type DeferredValidation = () => ConnectorResponseET[ValidationResult]

  val maxRows: Int

  def validateUpdateDefinition(tableName: String, updateData: Seq[UpdateDefinition], connector: Connector)(
      implicit ec: ExecutionContext
  ): ConnectorResponse[ValidationResult] = {
    runValidations(
      Seq(
        () => validateUpdateFormat(updateData),
        () => validateTableExistsAndNotView(tableName, connector),
        () => validateUpdateFields(tableName, updateData, connector)
      )
    ).value
  }

  def validateInsertData(tableName: String, dataToInsert: Seq[Record], connector: Connector)(
      implicit ec: ExecutionContext
  ): ConnectorResponse[ValidationResult] = {
    val validationResult = runValidations(
      Seq(
        () => validateFormat(dataToInsert),
        () => validateTableExistsAndNotView(tableName, connector),
        () => validateFieldExistence(tableName, dataToInsert.head.keySet, connector)
      )
    ) map {
      case EmptyData             => Valid
      case otherValidationResult => otherValidationResult
    }

    validationResult.value
  }

  def validateDeleteCriteria(tableName: String, criteria: Seq[Criteria], connector: Connector)(
      implicit ec: ExecutionContext
  ): ConnectorResponse[ValidationResult] = {
    runValidations(
      Seq(
        () => validateFormat(criteria),
        () => validateTableExistsAndNotView(tableName, connector),
        () => validateFieldExistence(tableName, criteria.head.keySet, connector),
        () => validateIndices(tableName, criteria.head.keySet, connector)
      )
    ).value
  }

  def validateSearchCriteria(tableName: String, criteria: Criteria, connector: Connector)(
      implicit ec: ExecutionContext
  ): ConnectorResponse[ValidationResult] = {
    runValidations(
      Seq(
        () => validateEmptyCriteria(criteria),
        () => validateTableExists(tableName, connector),
        () => validateFieldExistence(tableName, criteria.keySet, connector),
        () => validateIndices(tableName, criteria.keySet, connector)
      )
    ).value
  }

  private def validateEmptyCriteria(data: Criteria)(
      implicit ec: ExecutionContext
  ): ConnectorResponseET[ValidationResult] =
    EitherT.rightT[Future, ConnectorError] {
      if (data.isEmpty) EmptyData else Valid
    }

  private def runValidations(
      validations: Seq[DeferredValidation]
  )(implicit ec: ExecutionContext): ConnectorResponseET[ValidationResult] = {
    if (validations.isEmpty) {
      EitherT.rightT[Future, ConnectorError](Valid)
    } else {
      val validation = validations.head.apply()

      validation flatMap {
        case Valid            => runValidations(validations.tail)
        case validationResult => EitherT.rightT[Future, ConnectorError](validationResult)
      }
    }
  }

  private def validateFieldExistence(tableName: String, updateData: Seq[UpdateDefinition], connector: Connector)(
      implicit ec: ExecutionContext
  ): ConnectorResponseET[ValidationResult] = {
    if (updateData.isEmpty) {
      EitherT.rightT[Future, ConnectorError](EmptyData)
    } else {
      val fields = updateData.head.search.keySet ++ updateData.head.update.keySet
      validateFieldExistence(tableName, fields, connector)
    }
  }

  private def validateFieldExistence(tableName: String, fields: Set[String], connector: Connector)(
      implicit ec: ExecutionContext
  ): ConnectorResponseET[ValidationResult] = {
    EitherT(connector.listFields(tableName)).map { columns =>
      val nonExistingFields = fields.map(_.toLowerCase).diff(columns.map(_.name.toLowerCase).toSet)
      if (nonExistingFields.isEmpty) {
        Valid
      } else {
        NonExistingFields(fields.filter(field => nonExistingFields.contains(field.toLowerCase)))
      }
    }
  }

  private def validateTableExists(tableName: String, connector: Connector)(
      implicit ec: ExecutionContext
  ): ConnectorResponseET[ValidationResult] = {
    validateTableExistsAndIfView(tableName, connector, canBeView = true)
  }

  private def validateTableExistsAndNotView(tableName: String, connector: Connector)(
      implicit ec: ExecutionContext
  ): ConnectorResponseET[ValidationResult] = {
    validateTableExistsAndIfView(tableName, connector, canBeView = false)
  }

  private def validateTableExistsAndIfView(tableName: String, connector: Connector, canBeView: Boolean)(
      implicit ec: ExecutionContext
  ): ConnectorResponseET[ValidationResult] = {
    EitherT(connector.listTables()).map { tableModels =>
      tableModels.find(tableModel => tableModel.name == tableName) match {
        case Some(table) => if (!canBeView && table.isView) InvalidOperationOnView else Valid
        case None        => NonExistingTable
      }
    }
  }

  private def validateUpdateFields(tableName: String, updateData: Seq[UpdateDefinition], connector: Connector)(
      implicit ec: ExecutionContext
  ): ConnectorResponseET[ValidationResult] = {
    validateFieldExistence(tableName, updateData, connector) flatMap {
      case Valid            => validateIndices(tableName, updateData.head.search.keySet, connector)
      case validationResult => EitherT.rightT[Future, ConnectorError](validationResult)
    }
  }

  private def validateIndices(tableName: String, keyFields: Set[String], connector: Connector)(
      implicit ec: ExecutionContext
  ): ConnectorResponseET[ValidationResult] = {
    EitherT(connector.isOptimized(tableName, keyFields.toList))
      .map(isOptimized => if (isOptimized) Valid else NoIndexOnFields)
  }

  private def validateFormat(
      data: Seq[Record]
  )(implicit ec: ExecutionContext): ConnectorResponseET[ValidationResult] =
    EitherT.rightT[Future, ConnectorError] {
      if (data.size > maxRows) {
        TooManyRows
      } else if (data.isEmpty) {
        EmptyData
      } else if (!areAllKeysTheSame(data)) {
        DifferentFields
      } else {
        Valid
      }
    }

  private def areAllKeysTheSame(dataToInsert: Seq[Record]): Boolean = {
    val firstRecordsKeySet = dataToInsert.head.keySet
    dataToInsert.forall(_.keySet == firstRecordsKeySet)
  }

  private def validateUpdateFormat(
      updateData: Seq[UpdateDefinition]
  )(implicit ec: ExecutionContext): ConnectorResponseET[ValidationResult] =
    EitherT.rightT[Future, ConnectorError] {
      if (updateData.size > maxRows) {
        TooManyRows
      } else if (updateData.isEmpty) {
        EmptyData
      } else if (hasEmptyCriteria(updateData)) {
        EmptyCriteria
      } else if (hasEmptyData(updateData)) {
        EmptyData
      } else if (!areAllCriteriaFieldsTheSame(updateData)) {
        DifferentFields
      } else if (!areAllUpdateFieldsTheSame(updateData)) {
        DifferentFields
      } else {
        Valid
      }
    }

  private def areAllCriteriaFieldsTheSame(data: Seq[UpdateDefinition]): Boolean = {
    val firstRecordCriteriaKeySet = data.head.search.keySet
    data.forall(_.search.keySet == firstRecordCriteriaKeySet)
  }

  private def areAllUpdateFieldsTheSame(data: Seq[UpdateDefinition]): Boolean = {
    val firstRecordUpdateKeySet = data.head.update.keySet
    data.forall(_.update.keySet == firstRecordUpdateKeySet)
  }

  private def hasEmptyCriteria(updateData: Seq[UpdateDefinition]): Boolean = updateData.exists(_.search.isEmpty)

  private def hasEmptyData(updateData: Seq[UpdateDefinition]): Boolean = updateData.exists(_.update.isEmpty)
}

object ValidateDataManipulation extends ValidateDataManipulation {

  val maxRows = 1000

  sealed trait ValidationResult

  object ValidationResult {
    case object Valid            extends ValidationResult
    case object DifferentFields  extends ValidationResult
    case object EmptyData        extends ValidationResult
    case object TooManyRows      extends ValidationResult
    case object NonExistingTable extends ValidationResult

    case class NonExistingFields(fields: Set[String]) extends ValidationResult

    case object NoIndexOnFields extends ValidationResult

    case object EmptyCriteria extends ValidationResult

    case object InvalidOperationOnView extends ValidationResult

    case class ValidationFailed(message: String) extends ValidationResult
  }

}
