package com.emarsys.rdb.connector.common.models

import com.emarsys.rdb.connector.common.models.SimpleSelect.{And, EqualToValue, Or, WhereCondition}
import com.emarsys.rdb.connector.common.models.ValidateGroupLimitableQuery.GroupLimitValidationResult

trait ValidateGroupLimitableQuery {

  import ValidateGroupLimitableQuery.GroupLimitValidationResult._

  def groupLimitableQueryValidation(simpleSelect: SimpleSelect): GroupLimitValidationResult = {
    def validateOr(list: Seq[WhereCondition]): GroupLimitValidationResult = {
      val flatted = list.flatMap {
        case And(l) => l
        case x => Seq(x)
      }
      val filtered = flatted.collect {
        case x: EqualToValue => x
      }
      if (filtered.size != flatted.size) {
        NotGroupable
      } else {
        val grouped: Map[String, Seq[EqualToValue]] = filtered.groupBy(_.field.f)
        val sizes = grouped.map(_._2.size)
        sizes.headOption.fold[GroupLimitValidationResult](NotGroupable) { head =>
          if (sizes.forall(_ == head) && head == list.size) {
            Groupable(grouped.keys.toSeq)
          } else {
            NotGroupable
          }
        }
      }
    }


    simpleSelect.where.fold[GroupLimitValidationResult](Simple) {
      case Or(list) if list.size < 2 => Simple
      case Or(list) => validateOr(list)
      case _ => Simple
    }
  }


}


object ValidateGroupLimitableQuery extends ValidateGroupLimitableQuery {


  sealed trait GroupLimitValidationResult

  object GroupLimitValidationResult {

    case object Simple extends GroupLimitValidationResult
    case class Groupable(references: Seq[String]) extends GroupLimitValidationResult
    case object NotGroupable extends GroupLimitValidationResult

  }

}
