package com.emarsys.rdb.connector.bigquery.stream

import akka.NotUsed
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model._
import akka.stream._
import akka.stream.scaladsl.{GraphDSL, Source, Zip}
import com.emarsys.rdb.connector.bigquery.GoogleSession
import com.emarsys.rdb.connector.bigquery.stream.downstreamfinishhandler.UpstreamFailureHandler
import com.emarsys.rdb.connector.bigquery.stream.pagetoken.{AddPageToken, EndOfStreamDetector}
import com.emarsys.rdb.connector.bigquery.stream.parser.{PagingInfo, Parser}
import com.emarsys.rdb.connector.bigquery.stream.sendrequest.SendRequestWithOauthHandling
import com.emarsys.rdb.connector.bigquery.stream.util.{Delay, FlowInitializer}
import spray.json.JsObject

import scala.concurrent.ExecutionContext

object BigQueryStreamSource {
  def apply[T](
      httpRequest: HttpRequest,
      parserFn: JsObject => Option[T],
      googleSession: GoogleSession,
      http: HttpExt,
      upstreamFinishFn: ((Boolean, PagingInfo)) => Unit = (_: (Boolean, PagingInfo)) => ()
  )(
      implicit mat: Materializer
  ): Source[T, NotUsed] =
    Source.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      implicit val ec: ExecutionContext = mat.executionContext

      val in                     = builder.add(Source.repeat(httpRequest))
      val requestSender          = builder.add(SendRequestWithOauthHandling(googleSession, http))
      val parser                 = builder.add(Parser(parserFn))
      val upstreamFailureHandler = builder.add(UpstreamFailureHandler[(Boolean, PagingInfo)](upstreamFinishFn))
      val endOfStreamDetector    = builder.add(EndOfStreamDetector())
      val flowInitializer        = builder.add(FlowInitializer((false, PagingInfo(None, None))))
      val delay                  = builder.add(Delay[(Boolean, PagingInfo)](_._1, 60))
      val zip                    = builder.add(Zip[HttpRequest, (Boolean, PagingInfo)]())
      val addPageTokenNode       = builder.add(AddPageToken())

      in ~> zip.in0
      requestSender ~> parser.in
      parser.out1 ~> upstreamFailureHandler
      upstreamFailureHandler ~> endOfStreamDetector
      endOfStreamDetector ~> delay
      delay ~> flowInitializer
      flowInitializer ~> zip.in1
      zip.out ~> addPageTokenNode
      addPageTokenNode ~> requestSender

      SourceShape(parser.out0)

    /*
        +--------+           +------------+          +-------+         +------+
        |Request |           |AddPageToken|          |Request|         |Parser|
        |Repeater+---------->+            +--------->+Sender +-------->+      +-----+(response)+----->
        |        |           |            |          |       |         |      |
        +--------+           +-----+------+          +-------+         +---+--+
                                   ^                                       |
                                   |                                       v
                                   |     +-----------+                +----+------+
                                   |     |   Flow    |                | UpStream  |
                                   +<----+Initializer|                |  Finish   |
                                   |     | (single)  |                |  Handler  |
                                   |     +-----------+                +----+------+
                                   |                                       |
                                   |       +-----+       +-----------+     |
                                   |       |Delay|       |EndOfStream|     |
                                   +-------+     +<------+  Detector +<----+
                                           |     |       |           |
                                           +-----+       +-----------+
     */
    })
}
