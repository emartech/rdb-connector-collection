package com.emarsys.rdb.connector.common.models

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.Models.MetaData
import com.emarsys.rdb.connector.common.defaults.{
  DefaultFieldValueConverters,
  FieldValueConverters,
  GroupWithLimitStage
}
import com.emarsys.rdb.connector.common.models.DataManipulation.{Criteria, Record, UpdateDefinition}
import com.emarsys.rdb.connector.common.models.Errors.{FailedValidation, SimpleSelectIsNotGroupableFormat}
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import com.emarsys.rdb.connector.common.models.TableSchemaDescriptors._
import com.emarsys.rdb.connector.common.{ConnectorResponse, notImplementedOperation}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

trait Connector {

  protected val validator = new DataManipulationValidator(RawDataValidator)

  implicit val executionContext: ExecutionContext
  protected val fieldValueConverters: FieldValueConverters       = DefaultFieldValueConverters
  protected val groupLimitValidator: ValidateGroupLimitableQuery = ValidateGroupLimitableQuery

  val isErrorRetryable: PartialFunction[Throwable, Boolean] = {
    case _ => false
  }

  def close(): Future[Unit]

  def innerMetrics(): String = {
    "{}"
  }

  def testConnection(): ConnectorResponse[Unit] =
    notImplementedOperation(generateNotImplementedMessage("testConnection"))

  def listTables(): ConnectorResponse[Seq[TableModel]] =
    notImplementedOperation(generateNotImplementedMessage("listTables"))

  def listTablesWithFields(): ConnectorResponse[Seq[FullTableModel]] =
    notImplementedOperation(generateNotImplementedMessage("listTablesWithFields"))

  def listFields(table: String): ConnectorResponse[Seq[FieldModel]] =
    notImplementedOperation(generateNotImplementedMessage("listFields"))

  def isOptimized(table: String, fields: Seq[String]): ConnectorResponse[Boolean] =
    notImplementedOperation(generateNotImplementedMessage("isOptimized"))

  def simpleSelect(select: SimpleSelect, timeout: FiniteDuration): ConnectorResponse[Source[Seq[String], NotUsed]] =
    notImplementedOperation(generateNotImplementedMessage("simpleSelect"))

  def rawSelect(
      rawSql: String,
      limit: Option[Int],
      timeout: FiniteDuration
  ): ConnectorResponse[Source[Seq[String], NotUsed]] =
    notImplementedOperation(generateNotImplementedMessage("rawSelect"))

  def validateRawSelect(rawSql: String): ConnectorResponse[Unit] =
    notImplementedOperation(generateNotImplementedMessage("validateRawSelect"))

  def analyzeRawSelect(rawSql: String): ConnectorResponse[Source[Seq[String], NotUsed]] =
    notImplementedOperation(generateNotImplementedMessage("analyzeRawSelect"))

  def projectedRawSelect(
      rawSql: String,
      fields: Seq[String],
      limit: Option[Int],
      timeout: FiniteDuration,
      allowNullFieldValue: Boolean = false
  ): ConnectorResponse[Source[Seq[String], NotUsed]] =
    notImplementedOperation(generateNotImplementedMessage("projectedRawSelect"))

  def validateProjectedRawSelect(rawSql: String, fields: Seq[String]): ConnectorResponse[Unit] =
    notImplementedOperation(generateNotImplementedMessage("validateProjectedRawSelect"))

  def rawQuery(rawSql: String, timeout: FiniteDuration): ConnectorResponse[Int] =
    notImplementedOperation(generateNotImplementedMessage("rawQuery"))

  final def selectWithGroupLimit(
      select: SimpleSelect,
      groupLimit: Int,
      timeout: FiniteDuration
  ): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    import ValidateGroupLimitableQuery.GroupLimitValidationResult._
    groupLimitValidator.validate(select) match {
      case Simple                => simpleSelect(select.copy(limit = Option(groupLimit)), timeout)
      case Groupable(references) => runSelectWithGroupLimit(select, groupLimit, references, timeout)
      case NotGroupable          => Future.successful(Left(SimpleSelectIsNotGroupableFormat(select.toString)))
    }
  }

  protected def runSelectWithGroupLimit(
      select: SimpleSelect,
      groupLimit: Int,
      references: Seq[String],
      timeout: FiniteDuration
  ): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    simpleSelect(select, timeout).map(_.map(_.via(GroupWithLimitStage(references, groupLimit))))
  }

  protected def rawUpdate(tableName: String, definitions: Seq[UpdateDefinition]): ConnectorResponse[Int] =
    notImplementedOperation(generateNotImplementedMessage("rawUpdate"))

  protected def rawInsertData(tableName: String, definitions: Seq[Record]): ConnectorResponse[Int] =
    notImplementedOperation(generateNotImplementedMessage("rawInsertData"))

  protected def rawReplaceData(tableName: String, definitions: Seq[Record]): ConnectorResponse[Int] =
    notImplementedOperation(generateNotImplementedMessage("rawReplaceData"))

  protected def rawUpsert(tableName: String, definitions: Seq[Record]): ConnectorResponse[Int] =
    notImplementedOperation(generateNotImplementedMessage("rawUpsert"))

  protected def rawDelete(tableName: String, criteria: Seq[Criteria]): ConnectorResponse[Int] =
    notImplementedOperation(generateNotImplementedMessage("rawDelete"))

  protected def rawSearch(
      tableName: String,
      criteria: Criteria,
      limit: Option[Int],
      timeout: FiniteDuration
  ): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    val query = SimpleSelect(
      fields = AllField,
      table = TableName(tableName),
      where = Some(createWhereCondition(criteria)),
      limit = limit
    )

    simpleSelect(query, timeout)
  }

  private def createWhereCondition(criteria: Criteria): WhereCondition = {
    import com.emarsys.rdb.connector.common.defaults.FieldValueConverter._
    import fieldValueConverters._

    And(
      criteria
        .mapValues(_.toSimpleSelectValue)
        .map {
          case (field, Some(value)) => EqualToValue(FieldName(field), value)
          case (field, None)        => IsNull(FieldName(field))
        }
        .toSeq
    )
  }

  final def update(tableName: String, definitions: Seq[UpdateDefinition]): ConnectorResponse[Int] = {
    validateAndExecute(validator.validateUpdateDefinition, tableName, rawUpdate, definitions)
  }

  final def insertIgnore(tableName: String, records: Seq[Record]): ConnectorResponse[Int] = {
    validateAndExecute(validator.validateInsertData, tableName, rawInsertData, records)
  }

  final def replaceData(tableName: String, records: Seq[Record]): ConnectorResponse[Int] = {
    validateAndExecute(validator.validateInsertData, tableName, rawReplaceData, records)
  }

  final def upsert(tableName: String, definitions: Seq[Record]): ConnectorResponse[Int] = {
    validateAndExecute(validator.validateInsertData, tableName, rawUpsert, definitions)
  }

  final def delete(tableName: String, criteria: Seq[Criteria]): ConnectorResponse[Int] = {
    validateAndExecute(validator.validateDeleteCriteria, tableName, rawDelete, criteria)
  }

  final def search(
      tableName: String,
      criteria: Criteria,
      limit: Option[Int],
      timeout: FiniteDuration
  ): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    validateAndExecute(validator.validateSearchCriteria, tableName, rawSearch(_, _, limit, timeout), criteria)
  }

  private def validateAndExecute[A, C](
      validationFn: (String, C, Connector) => ConnectorResponse[ValidationResult],
      tableName: String,
      executionFn: (String, C) => ConnectorResponse[A],
      data: C
  ): ConnectorResponse[A] = {
    validationFn(tableName, data, this).flatMap {
      case Right(ValidationResult.Valid) => executionFn(tableName, data)
      case Right(failedValidationResult) => Future.successful(Left(FailedValidation(failedValidationResult)))
      case Left(ex)                      => Future.successful(Left(ex))
    }
  }

  private def generateNotImplementedMessage(operation: String): String = {
    s"$operation not implemented"
  }
}

trait ConnectorCompanion {
  def meta(): MetaData
}
