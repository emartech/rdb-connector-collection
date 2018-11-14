package com.emarsys.rdb.connector.bigquery.stream.util

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.{Concat, GraphDSL, Source}

object FlowInitializer {
  def apply[T](initialValue: T): Graph[FlowShape[T, T], NotUsed] = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val concat = builder.add(Concat[T](2))

    Source.single(initialValue) ~> concat.in(0)

    new FlowShape(concat.in(1), concat.out)
  }

}
