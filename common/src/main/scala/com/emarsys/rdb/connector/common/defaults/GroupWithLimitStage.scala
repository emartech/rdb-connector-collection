package com.emarsys.rdb.connector.common.defaults

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Source}

object GroupWithLimitStage {

  def apply[K](references: Seq[String], groupLimit: Int): Flow[Seq[String], Seq[String], NotUsed] = {
    val upperRefs = references.map(_.toUpperCase)

    def groupKey(xs: Seq[(String, String)]) =
      xs.filter(x => upperRefs.contains(x._1.toUpperCase)).map(_._2)

    Flow[Seq[String]]
      .prefixAndTail(1)
      .flatMapConcat {
        case (head, tail) =>
          head.headOption.fold(Source.empty[Seq[(String, String)]]) { header =>
            tail.map(row => header zip row)
          }
      }
      .groupBy(1024, groupKey)
      .take(groupLimit)
      .mergeSubstreams
      .statefulMapConcat(flattenBackToCsvStyle)
  }

  private def flattenBackToCsvStyle: () => Seq[(String, String)] => List[Seq[String]] = { () =>
    var wasHeader = false
    (l: Seq[(String, String)]) => {
      if (wasHeader) {
        List(l.map(_._2))
      } else {
        wasHeader = true
        List(l.map(_._1), l.map(_._2))
      }
    }
  }
}
