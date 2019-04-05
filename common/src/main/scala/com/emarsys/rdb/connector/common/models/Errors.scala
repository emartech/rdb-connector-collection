package com.emarsys.rdb.connector.common.models

import com.emarsys.rdb.connector.common.models.ValidateDataManipulation.ValidationResult

object Errors {

  sealed abstract class ConnectorError(message: String = "") extends Exception(message)

  case class ConnectionError(error: Throwable)                 extends ConnectorError(error.toString)
  case class ConnectionConfigError(message: String)            extends ConnectorError(message)
  case class ErrorWithMessage(message: String)                 extends ConnectorError(message)
  case class CommunicationsLinkFailure(message: String)        extends ConnectorError(message)
  case class TableNotFound(table: String)                      extends ConnectorError(s"Table not found: $table")
  case class SqlSyntaxError(message: String)                   extends ConnectorError(message)
  case class AccessDeniedError(message: String)                extends ConnectorError(message)
  case class ConnectionTimeout(message: String)                extends ConnectorError(message)
  case class CompletionTimeout(message: String)                extends ConnectorError(message)
  case class QueryTimeout(message: String)                     extends ConnectorError(message)
  case class NotImplementedOperation(message: String)          extends ConnectorError(message)
  case class SimpleSelectIsNotGroupableFormat(message: String) extends ConnectorError(message)
  case class TooManyQueries(message: String)                   extends ConnectorError(message)
  case class StuckPool(message: String)                        extends ConnectorError(message)
  case class FailedValidation(validationResult: ValidationResult)
      extends ConnectorError(s"Validation failed: $validationResult")
}
