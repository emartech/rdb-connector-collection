package com.emarsys.rdb.connector.bigquery.stream.sendrequest

import akka.NotUsed
import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.stream._
import akka.stream.scaladsl.{Concat, Flow, GraphDSL, Source}
import akka.util.Timeout
import com.emarsys.rdb.connector.bigquery.util.AkkaHttpPimps._

import scala.concurrent.{Await, ExecutionContext}

object ErrorSignalProcessor {
  def apply()(implicit timeout: Timeout, materializer: Materializer): Graph[FlowShape[HttpResponse, Boolean], NotUsed] = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val initialSource = builder.add(Source.single(true))
    val buffer = builder.add(Flow[HttpResponse].buffer(1, OverflowStrategy.backpressure))
    val errorMapper = builder.add(Flow[HttpResponse].map(response => response.status match {
      case StatusCodes.Unauthorized => false
      case otherStatus if otherStatus.isSuccess() => true
      case otherStatus => throw new IllegalStateException(s"Unexpected error in response: $otherStatus, ${getErrorBody(timeout, response)}")
    }))
    val concat = builder.add(Concat[Boolean](2))

    initialSource.out ~> concat.in(0)
    buffer ~> errorMapper ~> concat.in(1)

    FlowShape(buffer.in, concat.out)
  }

  private def getErrorBody(timeout: Timeout, response: HttpResponse)(implicit materializer: Materializer) = {
    implicit val ec: ExecutionContext = materializer.executionContext
    Await.result(response.entity.convertToString(), timeout.duration)
  }
}
