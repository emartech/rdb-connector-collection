package com.emarsys.rdb.connector.bigquery.stream.sendrequest

import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.scaladsl.{Flow, GraphDSL}
import akka.stream.{ActorMaterializer, FlowShape, Graph}
import akka.util.Timeout

import scala.concurrent.ExecutionContext

object SendRequestWithOauthHandling {
  def apply(tokenActor: ActorRef, http: HttpExt)(
    implicit tokenRequest: Timeout,
    ec: ExecutionContext,
    system: ActorSystem,
    mat: ActorMaterializer
  ): Graph[FlowShape[HttpRequest, HttpResponse], NotUsed] = {
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val signalBasedRepeater   = builder.add(new SignalBasedRepeater[HttpRequest]())
      val addGoogleOauthToken   = builder.add(EnrichRequestWithOauth(tokenActor))
      val sendHttpRequest       = builder.add(Flow[HttpRequest].mapAsync(1)(http.singleRequest(_)))
      val responseErrorSplitter = builder.add(Splitter[HttpResponse](_.status.isSuccess())(_ => true))
      val errorSignalProcessor  = builder.add(ErrorSignalProcessor())

      signalBasedRepeater.out ~> addGoogleOauthToken.in

      addGoogleOauthToken.out ~> sendHttpRequest.in

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
