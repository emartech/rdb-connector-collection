package com.emarsys.rdb.connector.common.models

import enumeratum._

import scala.collection.immutable

object Errors {

  sealed abstract class ConnectorError(message: String = "") extends Exception(message) {
    def withCause(cause: Throwable): ConnectorError = {
      this.initCause(cause)
      this
    }
  }

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
  case class TransientDbError(message: String)                 extends ConnectorError(message)
  case class FailedValidation(validationResult: ValidationResult)
      extends ConnectorError(s"Validation failed: $validationResult")

  sealed trait ErrorCategory extends EnumEntry
  object ErrorCategory extends Enum[ErrorCategory] {
    case object Timeout extends ErrorCategory

    override def values: immutable.IndexedSeq[ErrorCategory] = findValues
  }

  sealed trait ErrorName extends EnumEntry {
    def unapply(payload: ErrorPayload): Boolean = ErrorName.withNameOption(payload.error).isDefined
  }
  object ErrorName extends Enum[ErrorName] {
    case object QueryTimeout extends ErrorName

    override def values: immutable.IndexedSeq[ErrorName] = findValues
  }

  case class ErrorPayload(errorCategory: ErrorCategory, error: String, message: String) extends ConnectorError(message)
  object ErrorPayload {
    def apply(errorCategory: ErrorCategory, error: ErrorName, message: String): ErrorPayload =
      new ErrorPayload(errorCategory, error.entryName, message)
  }
}
