package com.emarsys.rdb.connector.bigquery.stream.sendrequest

import akka.NotUsed
import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.stream.{FlowShape, Graph, OverflowStrategy}
import akka.stream.scaladsl.{Concat, Flow, GraphDSL, Source}

object ErrorSignalProcessor {
  def apply(): Graph[FlowShape[HttpResponse, Boolean], NotUsed] = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val initialSource = builder.add(Source.single(true))
    val buffer = builder.add(Flow[HttpResponse].buffer(1, OverflowStrategy.backpressure))
    val errorMapper = builder.add(Flow[HttpResponse].map(_.status match {
      case StatusCodes.Unauthorized => false
      case otherStatus if otherStatus.isSuccess() => true
      case otherStatus => throw new IllegalStateException(s"Unexpected error in response, status: $otherStatus")
    }))
    val concat = builder.add(Concat[Boolean](2))

    initialSource.out ~> concat.in(0)
    buffer ~> errorMapper ~> concat.in(1)

    FlowShape(buffer.in, concat.out)
  }
}
