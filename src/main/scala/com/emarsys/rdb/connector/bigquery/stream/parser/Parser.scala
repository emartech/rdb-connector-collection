package com.emarsys.rdb.connector.bigquery.stream.parser

import akka.NotUsed
import akka.http.scaladsl.model.HttpResponse
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL}
import akka.stream.{FanOutShape2, FlowShape, Graph, Materializer}
import akka.util.ByteString
import spray.json._

import scala.concurrent.ExecutionContext

final case class PagingInfo(pageToken: Option[String], jobId: Option[String])

object Parser {
  def apply[T](parseFunction: JsObject => T)(implicit materializer: Materializer, ec: ExecutionContext): Graph[FanOutShape2[HttpResponse, T, PagingInfo], NotUsed] =
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val broadcast = builder.add(Broadcast[JsObject](2))
      val parseMap = builder.add(Flow[JsObject].map(parseFunction(_)))

      val bodyJsonParse: FlowShape[HttpResponse, JsObject] = builder.add(Flow[HttpResponse].mapAsync(1)(response => {
        response.entity.dataBytes
          .runFold(ByteString(""))(_ ++ _)
          .map {
            _.utf8String match {
              case "" => JsObject()
              case nonEmptyString => nonEmptyString.parseJson.asJsObject
            }
          }
      }))

      val pageInfoProvider = builder.add(Flow[JsObject].map(getPageInfo))

      bodyJsonParse.out ~> broadcast.in
      broadcast.out(0) ~> parseMap.in
      broadcast.out(1) ~> pageInfoProvider.in


      new FanOutShape2(bodyJsonParse.in, parseMap.out, pageInfoProvider.out)
    }

  private def getPageInfo[T](jsObject: JsObject): PagingInfo =  {
    val pageToken = jsObject.fields.get("pageToken").map(_.asInstanceOf[JsString].value)
    val jobId = jsObject.fields.get("jobReference").flatMap(_.asJsObject().fields.get("jobId")).map(_.asInstanceOf[JsString].value)

    PagingInfo(pageToken, jobId)
  }
}
