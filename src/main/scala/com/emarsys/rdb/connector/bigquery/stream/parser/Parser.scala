package com.emarsys.rdb.connector.bigquery.stream.parser

import akka.NotUsed
import akka.http.scaladsl.model.HttpResponse
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL}
import akka.stream.{FanOutShape2, Graph, Materializer}
import akka.util.ByteString
import spray.json._

import scala.concurrent.ExecutionContext

object Parser {
  def apply[T](parseFunction: JsObject => T)(implicit materializer: Materializer, ec: ExecutionContext): Graph[FanOutShape2[HttpResponse, T, Option[String]], NotUsed] =
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val broadcast = builder.add(Broadcast[JsObject](2))
      val parseMap = builder.add(Flow[JsObject].map(parseFunction(_)))

      val bodyJsonParse = builder.add(Flow[HttpResponse].mapAsync(1)(request => {
        request.entity.dataBytes.runFold(ByteString(""))(_ ++ _).map(_.utf8String.parseJson.asJsObject)
      }))

      val findPageToken = builder.add(Flow[JsObject].map(getPageTokenFromResponse))

      bodyJsonParse.out ~> broadcast.in
      broadcast.out(0) ~> parseMap.in
      broadcast.out(1) ~> findPageToken.in


      new FanOutShape2(bodyJsonParse.in, parseMap.out, findPageToken.out)
    }

  private def getPageTokenFromResponse[T](jsObject: JsObject) = for {
    nextPageToken <- jsObject.fields.get("nextPageToken")
  } yield nextPageToken.asInstanceOf[JsString].value
}
