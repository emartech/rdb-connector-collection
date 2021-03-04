package com.emarsys.rdb.connector.common.models

import enumeratum._

import scala.collection.immutable

object Errors {
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
    def unapply(errorName: String): Boolean = ErrorName.withNameOption(errorName).contains(this)
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
    case object QueryRejected           extends ErrorName
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

  case class DatabaseError(
      errorCategory: ErrorCategory,
      errorName: ErrorName,
      message: String,
      cause: Option[Throwable] = None,
      context: Option[Context] = None
  ) extends Exception(message, cause.orNull) {
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
}
