package com.emarsys.rdb.connector.bigquery.stream.pagetoken

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.{Concat, Flow, GraphDSL, Source}
import com.emarsys.rdb.connector.bigquery.stream.parser.PagingInfo

object PageTokenGenerator {

  def apply(): Graph[FlowShape[PagingInfo, PagingInfo], NotUsed] = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val concat = builder.add(Concat[PagingInfo](2))
    val takeWhileSome = builder.add(Flow[PagingInfo].takeWhile(_.pageToken.isDefined))

    Source.single(PagingInfo(None, None)) ~> concat.in(0)
    takeWhileSome.out ~> concat.in(1)

    new FlowShape(takeWhileSome.in, concat.out)
  }

}
