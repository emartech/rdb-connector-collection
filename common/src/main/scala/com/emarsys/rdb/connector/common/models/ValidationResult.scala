package com.emarsys.rdb.connector.common.models

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
