package com.emarsys.rdb.connector.bigquery.stream

import akka.NotUsed
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL}
import akka.stream.{Graph, UniformFanOutShape}

object BooleanSplitter {

  def apply[T](f: T => Boolean): Graph[UniformFanOutShape[T, T], NotUsed] = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val broadcast = builder.add(Broadcast[T](2))
    val filterTrue = builder.add(Flow[T].filter(f(_)))
    val filterFalse = builder.add(Flow[T].filter(!f(_)))

    broadcast.out(0) ~> filterTrue.in
    broadcast.out(1) ~> filterFalse.in

    UniformFanOutShape(broadcast.in, filterTrue.out, filterFalse.out)
  }

}
