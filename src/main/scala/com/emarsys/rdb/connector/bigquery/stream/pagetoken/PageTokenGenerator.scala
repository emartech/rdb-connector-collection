package com.emarsys.rdb.connector.bigquery.stream.pagetoken

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.{Concat, Flow, GraphDSL, Source}

object PageTokenGenerator {

  def apply[T](): Graph[FlowShape[Option[T], Option[T]], NotUsed] = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val concat = builder.add(Concat[Option[T]](2))
    val takeWhileSome = builder.add(Flow[Option[T]].takeWhile(_.isDefined))

    Source.single(None) ~> concat.in(0)
    takeWhileSome.out ~> concat.in(1)

    new FlowShape(takeWhileSome.in, concat.out)
  }

}
