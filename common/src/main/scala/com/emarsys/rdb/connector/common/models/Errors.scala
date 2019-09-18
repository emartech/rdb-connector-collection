package com.emarsys.rdb.connector.common.models

import com.emarsys.rdb.connector.common.defaults.ErrorConverter
import enumeratum._

import scala.collection.immutable
import scala.reflect._

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
    case object Validation          extends ErrorCategory
    case object Timeout             extends ErrorCategory
    case object RateLimit           extends ErrorCategory
    case object FatalQueryExecution extends ErrorCategory
    case object Transient           extends ErrorCategory
    case object Internal            extends ErrorCategory
    case object Unknown             extends ErrorCategory

    override def values: immutable.IndexedSeq[ErrorCategory] = findValues
  }

  sealed trait ValidationError

  sealed trait ErrorName extends EnumEntry {
    def unapply(payload: ErrorPayload): Boolean = unapply(payload.errorName)
    def unapply(errorName: String): Boolean     = ErrorName.withNameOption(errorName).contains(this)
  }
  object ErrorName extends Enum[ErrorName] {
    // ======================================
    //              Validation
    // ======================================
    case object MissingFields          extends ErrorName with ValidationError
    case object DifferentFields        extends ErrorName with ValidationError
    case object EmptyData              extends ErrorName with ValidationError
    case object TooManyRows            extends ErrorName with ValidationError
    case object NonExistingTable       extends ErrorName with ValidationError
    case object NoIndexOnFields        extends ErrorName with ValidationError
    case object EmptyCriteria          extends ErrorName with ValidationError
    case object InvalidOperationOnView extends ErrorName with ValidationError
    case object ValidationFailed       extends ErrorName with ValidationError
    // ======================================
    //              Timeout
    // ======================================
    case object ConnectionTimeout extends ErrorName
    case object CompletionTimeout extends ErrorName
    case object QueryTimeout      extends ErrorName
    // ======================================
    //              RateLimit
    // ======================================
    case object TooManyQueries extends ErrorName
    case object StuckPool      extends ErrorName
    case object Failsafe       extends ErrorName
    // ======================================
    //              FatalQueryExecution
    // ======================================
    case object TableNotFound           extends ErrorName
    case object SqlSyntaxError          extends ErrorName
    case object AccessDeniedError       extends ErrorName
    case object NotImplementedOperation extends ErrorName
    case object SSLError                extends ErrorName
    case object ConnectionConfigError   extends ErrorName
    // ======================================
    //              Transient
    // ======================================
    case object CommunicationsLinkFailure extends ErrorName
    case object TransientDbError          extends ErrorName
    // ======================================
    //              Internal
    // ======================================
    case object SimpleSelectIsNotGroupableFormat extends ErrorName
    // ======================================
    //              Unknown
    // ======================================
    case object Unknown extends ErrorName

    override def values: immutable.IndexedSeq[ErrorName] = findValues
  }

  case class Cause(message: String)
  case class ErrorPayload(
      errorCategory: ErrorCategory,
      errorName: String,
      message: String,
      causes: List[Cause],
      context: Option[Context]
  ) extends ConnectorError(message) {

    override def toString: String = {
      s"ErrorPayload($errorCategory,$errorName,$message,$causes,$context)"
    }

    def maybeContext[C <: Context: ClassTag]: Option[C] = context collect { case c: C => c }
    def ensureContext[C <: Context: ClassTag]: C        = maybeContext[C].getOrElse(throw new ContextMismatch[C](context))
  }

  object ErrorPayload {
    def apply(
        errorCategory: ErrorCategory,
        errorName: ErrorName,
        message: String,
        cause: Option[Throwable],
        context: Option[Context]
    ): ErrorPayload = {
      val causes = cause.map(ErrorConverter.getCauseMessages).getOrElse(List.empty).map(Cause)
      ErrorPayload(errorCategory, errorName.entryName, message, causes, context)
    }

    def fromDatabaseError(databaseError: DatabaseError): ErrorPayload = {
      val causes = databaseError.cause.map(ErrorConverter.getCauseMessages).getOrElse(List.empty).map(Cause)
      ErrorPayload(
        databaseError.errorCategory,
        databaseError.errorName.entryName,
        databaseError.message,
        causes,
        databaseError.context
      )
    }
  }

  case class DatabaseError(
      errorCategory: ErrorCategory,
      errorName: ErrorName,
      message: String,
      cause: Option[Throwable] = None,
      context: Option[Context] = None
  ) extends ConnectorError(message) {
    override def toString: String = {
      s"DatabaseError($errorCategory,$errorName,$message,$cause,$context)"
    }
  }

  object DatabaseError {
    def apply(errorCategory: ErrorCategory, errorName: ErrorName, cause: Throwable): DatabaseError = DatabaseError(
      errorCategory,
      errorName,
      cause.getMessage,
      Some(cause),
      None
    )

    def validation[E <: ErrorName with ValidationError](errorName: E, context: Option[Context] = None): DatabaseError =
      DatabaseError(ErrorCategory.Validation, errorName, "", None, context)

  }

  sealed trait Context
  case class Fields(fields: List[String]) extends Context

  case class ContextMismatch[C <: Context: ClassTag](context: Option[Context]) extends Throwable {
    val expected = classTag[C].runtimeClass.getSimpleName
    val actual   = context.map(_.getClass().getSimpleName).getOrElse("None")

    override def getMessage(): String = s"Expected `$expected`, found `$actual`"
  }
}
