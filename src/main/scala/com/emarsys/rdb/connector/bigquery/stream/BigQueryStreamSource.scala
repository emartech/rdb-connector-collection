package com.emarsys.rdb.connector.bigquery.stream

import java.net.URLEncoder

import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model.{HttpEntity, HttpMethods, HttpRequest, Uri}
import akka.stream._
import akka.stream.scaladsl.{Flow, GraphDSL, Source, Zip}
import akka.util.Timeout
import com.emarsys.rdb.connector.bigquery.stream.pagetoken.PageTokenGenerator
import com.emarsys.rdb.connector.bigquery.stream.parser.{PagingInfo, Parser}
import com.emarsys.rdb.connector.bigquery.stream.sendrequest.SendRequestWithOauthHandling
import spray.json.JsObject
import com.emarsys.rdb.connector.bigquery.util.AkkaHttpPimps._

import scala.concurrent.ExecutionContext


object BigQueryStreamSource {
  def addPageToken(request: HttpRequest, pagingInfo: PagingInfo): HttpRequest = {
    val req = request.copy(uri = request.uri.withQuery(Uri.Query(pagingInfo.pageToken.map(token => s"pageToken=${URLEncoder.encode(token, "utf-8")}"))))
    pagingInfo.jobId.fold(req) { id =>
      val getReq = req.copy(
        method = HttpMethods.GET,
        entity = HttpEntity.Empty
      )
      getReq.copy(uri = req.uri.withPath(req.uri.path ?/ id))
    }
  }


  def apply[T](httpRequest: HttpRequest, parserFn: JsObject => T, tokenActor: ActorRef, http: HttpExt)
              (implicit timeout: Timeout, mat: ActorMaterializer): Source[T, NotUsed] = Source.fromGraph(GraphDSL.create() {
    implicit builder =>
      import GraphDSL.Implicits._

      implicit val system: ActorSystem = mat.system
      implicit val ec: ExecutionContext = mat.executionContext

      val in = builder.add(Source.repeat(httpRequest))
      val requestSender = builder.add(SendRequestWithOauthHandling(tokenActor, http))
      val parser = builder.add(Parser(parserFn))
      val pageTokenGenerator = builder.add(PageTokenGenerator())
      val zip = builder.add(Zip[HttpRequest, PagingInfo]())
      val addPageTokenNode = builder.add(Flow[(HttpRequest, PagingInfo)].map((addPageToken _).tupled))

      in ~> zip.in0
      requestSender.out ~> parser.in
      parser.out1 ~> pageTokenGenerator.in
      pageTokenGenerator.out ~> zip.in1
      zip.out ~> addPageTokenNode.in
      addPageTokenNode.out ~> requestSender.in

      SourceShape(parser.out0)

      /*
      +--------+           +------------+          +-------+         +------+
      |Request |           |AddPageToken|          |Request|         |Parser|
      |Repeater+---------->|            +--------->|Sender +-------->|      +------(response)------>
      |        |           |            |          |       |         |      |
      +--------+           +------------+          +-------+         +---+--+
                                  ^                                      |
                                  |                                      |
                                  |                                      |
                                  |               +---------+            |
                                  |               |PageToken|            |
                                  +---------------+Generator|<-----------+
                                                  |         |
                                                  +---------+
       */
  })
}
