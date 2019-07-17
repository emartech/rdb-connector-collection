package com.emarsys.rdb.connector.common.models

import com.emarsys.rdb.connector.common.models.SimpleSelect.{And, EqualToValue, Or, WhereCondition}
import com.emarsys.rdb.connector.common.models.ValidateGroupLimitableQuery.GroupLimitValidationResult

trait ValidateGroupLimitableQuery {

  import ValidateGroupLimitableQuery.GroupLimitValidationResult._

  def validate(simpleSelect: SimpleSelect): GroupLimitValidationResult = {
    simpleSelect.where.fold[GroupLimitValidationResult](Simple) {
      case Or(list) if list.size < 2 => Simple
      case Or(list)                  => validateOr(list)
      case _                         => Simple
    }
  }

  private def validateOr(conditions: Seq[WhereCondition]): GroupLimitValidationResult = {
    val flattedConditions = conditions.flatMap {
      case And(l) => l
      case x      => Seq(x)
    }
    val equalTos = flattedConditions.collect {
      case x: EqualToValue => x
    }
    if (equalTos.size != flattedConditions.size) {
      NotGroupable
    } else {
      val groupedEqualTos: Map[String, Seq[EqualToValue]] = equalTos.groupBy(_.field.f)
      val sizes                                           = groupedEqualTos.map(_._2.size)
      sizes.headOption.fold[GroupLimitValidationResult](NotGroupable) { firstSize =>
        if (sizes.forall(_ == firstSize) && firstSize == conditions.size) {
          Groupable(groupedEqualTos.keys.toSeq)
        } else {
          NotGroupable
        }
      }
    }
  }

}

object ValidateGroupLimitableQuery extends ValidateGroupLimitableQuery {

  sealed trait GroupLimitValidationResult

  object GroupLimitValidationResult {

    case object Simple                            extends GroupLimitValidationResult
    case class Groupable(references: Seq[String]) extends GroupLimitValidationResult
    case object NotGroupable                      extends GroupLimitValidationResult

  }

}
