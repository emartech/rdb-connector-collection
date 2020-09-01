package com.emarsys.rdb.connector.bigquery.stream.parser

import akka.NotUsed
import akka.http.scaladsl.model.HttpResponse
import akka.stream.{FanOutShape2, FlowShape, Graph, Materializer}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Zip}
import com.emarsys.rdb.connector.bigquery.util.AkkaHttpPimps._
import spray.json._

import scala.concurrent.ExecutionContext

final case class PagingInfo(pageToken: Option[String], jobId: Option[String])

object Parser {

  object ParserJsonProtocol extends DefaultJsonProtocol {

    case class Response(jobReference: Option[JobReference], pageToken: Option[String], nextPageToken: Option[String])

    case class JobReference(jobId: Option[String])

    implicit val jobReferenceFormat: RootJsonFormat[JobReference] = jsonFormat1(JobReference)
    implicit val responseFormat: RootJsonFormat[Response]         = jsonFormat3(Response)
  }

  def apply[T](parseFunction: JsObject => Option[T])(
      implicit materializer: Materializer,
      ec: ExecutionContext
  ): Graph[FanOutShape2[HttpResponse, T, (Boolean, PagingInfo)], NotUsed] = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val bodyJsonParse: FlowShape[HttpResponse, JsObject] = builder.add(Flow[HttpResponse].mapAsync(1)(parseHttpBody(_)))

    val parseMap         = builder.add(Flow[JsObject].map(parseFunction(_)))
    val pageInfoProvider = builder.add(Flow[JsObject].map(getPageInfo))

    val broadcast1 = builder.add(Broadcast[JsObject](2, true))
    val broadcast2 = builder.add(Broadcast[Option[T]](2, true))

    val filterNone = builder.add(Flow[Option[T]].mapConcat {
      case Some(value) => List(value)
      case None        => List()
    })
    val mapOptionToBool = builder.add(Flow[Option[T]].map(_.isEmpty))

    val zip = builder.add(Zip[Boolean, PagingInfo]())

    bodyJsonParse ~> broadcast1

    broadcast1.out(0) ~> parseMap
    broadcast1.out(1) ~> pageInfoProvider

    parseMap ~> broadcast2

    broadcast2.out(0) ~> filterNone
    broadcast2.out(1) ~> mapOptionToBool

    mapOptionToBool ~> zip.in0
    pageInfoProvider ~> zip.in1

    new FanOutShape2(bodyJsonParse.in, filterNone.out, zip.out)
  }

  private def parseHttpBody[T](response: HttpResponse)(implicit materializer: Materializer, ec: ExecutionContext) = {
    response.entity
      .convertToString()
      .map {
        case ""             => JsObject()
        case nonEmptyString => nonEmptyString.parseJson.asJsObject
      }
  }

  private def getPageInfo[T](jsObject: JsObject): PagingInfo = {
    import ParserJsonProtocol._

    val response = jsObject.convertTo[Response]

    val pageToken = response.pageToken orElse response.nextPageToken
    val jobId     = response.jobReference.flatMap(_.jobId)

    PagingInfo(pageToken, jobId)
  }
}
