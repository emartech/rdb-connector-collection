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

import scala.concurrent.ExecutionContext


object BigQueryStreamSource {
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
      val addPageToken = builder.add(Flow[(HttpRequest, PagingInfo)].map { case (request, PagingInfo(pageToken, jobId)) =>
        val req = request.copy(uri = request.uri.withQuery(Uri.Query(pageToken.map(token => s"pageToken=${URLEncoder.encode(token, "utf-8")}"))))
        jobId.fold(req) { id =>
          val getReq = req.copy(
            method = HttpMethods.GET,
            entity = HttpEntity.Empty
          )
          if (req.uri.path.endsWithSlash) {
            getReq.copy(uri = req.uri.withPath(req.uri.path + id))
          } else {
            getReq.copy(uri = req.uri.withPath(req.uri.path / id))
          }

        }
      })


      in ~> zip.in0
      requestSender.out ~> parser.in
      parser.out1 ~> pageTokenGenerator.in
      pageTokenGenerator.out ~> zip.in1
      zip.out ~> addPageToken.in
      addPageToken.out ~> requestSender.in

      SourceShape(parser.out0)
  })
}
