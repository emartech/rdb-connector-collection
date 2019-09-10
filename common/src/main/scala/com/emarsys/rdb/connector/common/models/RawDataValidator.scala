package com.emarsys.rdb.connector.common.models

import cats.data.EitherT
import cats.instances.future._
import com.emarsys.rdb.connector.common.ConnectorResponseET
import com.emarsys.rdb.connector.common.models.DataManipulation.{Criteria, Record, UpdateDefinition}
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorName, Fields}
import com.emarsys.rdb.connector.common.models.ValidationResult._

import scala.concurrent.ExecutionContext

trait RawDataValidator {
  import cats.syntax.flatMap._

  def validateEmptyCriteria(data: Criteria)(
      implicit ec: ExecutionContext
  ): ConnectorResponseET[ValidationResult] = {
    if (data.isEmpty)
      EitherT.leftT(DatabaseError.validation(ErrorName.EmptyData))
    else EitherT.rightT(Valid)
  }

  def validateFieldExistence(tableName: String, fields: Set[String], connector: Connector)(
      implicit ec: ExecutionContext
  ): ConnectorResponseET[ValidationResult] = {
    EitherT(connector.listFields(tableName))
      .flatMap { columns =>
        val lowerCasedFields  = fields.map(_.toLowerCase)
        val nonExistingFields = lowerCasedFields.diff(columns.map(_.name.toLowerCase).toSet)
        if (nonExistingFields.isEmpty) {
          EitherT.rightT(Valid)
        } else {
          val missingFields = lowerCasedFields.intersect(nonExistingFields).toList
          EitherT.leftT(
            DatabaseError.validation(ErrorName.MissingFields, Some(Fields(missingFields)))
          )
        }
      }
  }

  def validateTableExists(tableName: String, connector: Connector)(
      implicit ec: ExecutionContext
  ): ConnectorResponseET[ValidationResult] = {
    validateTableExistsAndIfView(tableName, connector, canBeView = true)
  }

  def validateTableExistsAndNotView(tableName: String, connector: Connector)(
      implicit ec: ExecutionContext
  ): ConnectorResponseET[ValidationResult] = {
    validateTableExistsAndIfView(tableName, connector, canBeView = false)
  }

  private def validateTableExistsAndIfView(tableName: String, connector: Connector, canBeView: Boolean)(
      implicit ec: ExecutionContext
  ): ConnectorResponseET[ValidationResult] = {
    EitherT(connector.listTables()).flatMap { tableModels =>
      tableModels.find(tableModel => tableModel.name == tableName) match {
        case Some(table) =>
          if (!canBeView && table.isView)
            EitherT.leftT(DatabaseError.validation(ErrorName.InvalidOperationOnView))
          else EitherT.rightT(Valid)
        case None => EitherT.leftT(DatabaseError.validation(ErrorName.NonExistingTable))
      }
    }
  }

  def validateUpdateFields(tableName: String, updateData: Seq[UpdateDefinition], connector: Connector)(
      implicit ec: ExecutionContext
  ): ConnectorResponseET[ValidationResult] = {
    updateData match {
      case Nil => EitherT.leftT(DatabaseError.validation(ErrorName.EmptyData))
      case first :: _ =>
        validateFieldExistence(tableName, fieldsOf(first), connector).flatMap { _ =>
          validateIndices(tableName, first.search.keySet, connector)
        }
    }
  }

  private def fieldsOf(updateDefinition: UpdateDefinition): Set[String] =
    updateDefinition.search.keySet ++ updateDefinition.update.keySet

  def validateIndices(tableName: String, keyFields: Set[String], connector: Connector)(
      implicit ec: ExecutionContext
  ): ConnectorResponseET[ValidationResult] = {
    EitherT(connector.isOptimized(tableName, keyFields.toList))
      .ifM(
        EitherT.rightT(Valid),
        EitherT.leftT(DatabaseError.validation(ErrorName.NoIndexOnFields))
      )
  }

  def validateFormat(
      data: Seq[Record],
      maxRows: Int
  )(implicit ec: ExecutionContext): ConnectorResponseET[ValidationResult] = {
    data match {
      case Nil => EitherT.leftT(DatabaseError.validation(ErrorName.EmptyData))
      case first :: _ =>
        if (data.size > maxRows) {
          EitherT.leftT(DatabaseError.validation(ErrorName.TooManyRows))
        } else if (!areAllKeysTheSameAs(first, data)) {
          EitherT.leftT(DatabaseError.validation(ErrorName.DifferentFields))
        } else {
          EitherT.rightT(Valid)
        }
    }
  }

  private def areAllKeysTheSameAs(compareWith: Record, dataToInsert: Seq[Record]): Boolean = {
    val firstRecordsKeySet = compareWith.keySet
    dataToInsert.forall(_.keySet == firstRecordsKeySet)
  }

  def validateUpdateFormat(
      updateData: Seq[UpdateDefinition],
      maxRows: Int
  )(implicit ec: ExecutionContext): ConnectorResponseET[ValidationResult] =
    if (updateData.size > maxRows) {
      EitherT.leftT(DatabaseError.validation(ErrorName.TooManyRows))
    } else if (updateData.isEmpty) {
      EitherT.leftT(DatabaseError.validation(ErrorName.EmptyData))
    } else if (hasEmptyCriteria(updateData)) {
      EitherT.leftT(DatabaseError.validation(ErrorName.EmptyCriteria))
    } else if (hasEmptyData(updateData)) {
      EitherT.leftT(DatabaseError.validation(ErrorName.EmptyData))
    } else if (!areAllCriteriaFieldsTheSame(updateData) || !areAllUpdateFieldsTheSame(updateData)) {
      EitherT.leftT(DatabaseError.validation(ErrorName.DifferentFields))
    } else {
      EitherT.rightT(Valid)
    }

  private def hasEmptyCriteria(updateData: Seq[UpdateDefinition]): Boolean = updateData.exists(_.search.isEmpty)

  private def hasEmptyData(updateData: Seq[UpdateDefinition]): Boolean = updateData.exists(_.update.isEmpty)

  private def areAllCriteriaFieldsTheSame(data: Seq[UpdateDefinition]): Boolean = {
    val firstRecordCriteriaKeySet = data.head.search.keySet
    data.forall(_.search.keySet == firstRecordCriteriaKeySet)
  }

  private def areAllUpdateFieldsTheSame(data: Seq[UpdateDefinition]): Boolean = {
    val firstRecordUpdateKeySet = data.head.update.keySet
    data.forall(_.update.keySet == firstRecordUpdateKeySet)
  }

}

object RawDataValidator extends RawDataValidator
