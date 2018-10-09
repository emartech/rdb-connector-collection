package com.emarsys.rdb.connector.common.defaults

import akka.NotUsed
import akka.stream.scaladsl.Flow

object GroupWithLimitStage {

  def apply[K](references: Seq[String], groupLimit: Int): Flow[Seq[String], Seq[String], NotUsed] = {
    def groupKey(xs: Seq[(String, String)]) =
      xs.filter(x => references.contains(x._1)).map(_._2)

    Flow[Seq[String]]
      .prefixAndTail(1)
      .flatMapConcat { case (head, tail) =>
          val header = head.head
          tail.map(x => header zip x)
      }
      .groupBy(1024, groupKey)
      .take(groupLimit)
      .mergeSubstreams
      .statefulMapConcat(flattenBackToCsvStyle)
  }

  private def flattenBackToCsvStyle: () => Seq[(String, String)] => List[Seq[String]] = {
    () =>
      var wasHeader = false
      (l: Seq[(String, String)]) => {
      if(wasHeader) {
        List(l.map(_._2))
      } else {
        wasHeader = true
        List(l.map(_._1), l.map(_._2))
      }
    }
  }
}
