package com.emarsys.rdb.connector.bigquery.stream

import akka.NotUsed
import akka.actor.ActorRef
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.{FlowShape, Graph}
import akka.stream.scaladsl.{Flow, GraphDSL}
import akka.util.Timeout

import scala.concurrent.ExecutionContext

object SendRequestWithOauthHandling {
  def apply(tokenActor: ActorRef)(implicit tokenRequest: Timeout, ec: ExecutionContext): Graph[FlowShape[HttpRequest, HttpResponse], NotUsed] = {
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val signalBasedRepeater = builder.add(new SignalBasedRepeater[HttpRequest]())
      val addGoogleOautToken = builder.add(EnrichRequestWithOauth(tokenActor))
      val sendHttpRequest = builder.add(Flow[HttpRequest].mapAsync(1)(request => Http().singleRequest(request)))
      val responseErrorSplitter = builder.add(BooleanSplitter[HttpResponse](_.status.isSuccess()))
      val errorSignalProcessor = builder.add(ErrorSignalProcessor())

      signalBasedRepeater.out ~> addGoogleOautToken.in

      addGoogleOautToken.out ~> sendHttpRequest.in

      sendHttpRequest.out ~> responseErrorSplitter.in

      responseErrorSplitter.out(1) ~> errorSignalProcessor.in

      errorSignalProcessor.out ~> signalBasedRepeater.in1

      FlowShape[HttpRequest, HttpResponse](signalBasedRepeater.in0, responseErrorSplitter.out(0))

      /*
                       +----------+     +------------+     +---------+     +----------+
                       | Signal   |     | Add Google |     | Send    |     | Response |
     ----(Request)---->| Based    |---->| OAuth      |---->| Http    |---->| Error    |----(OK: Response)--->
                   /-->| Repeater |     | Token      |     | Request |     | Splitter |
                   |   +----------+     +------------+     +---------+     +----------+
                   |                        +------------+                       |
                   |                        | Error      |                       |
                   \------------------------| Signal     |<----------------------/
                                            | Processor  |
                                            +------------+
       */
    }
  }
}
