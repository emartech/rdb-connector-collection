package com.emarsys.rdb.connector.bigquery.stream

import akka.NotUsed
import akka.actor.ActorRef
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model.{HttpRequest, Uri}
import akka.stream._
import akka.stream.scaladsl.{Flow, GraphDSL, Source, Zip}
import akka.util.Timeout
import com.emarsys.rdb.connector.bigquery.stream.pagetoken.PageTokenGenerator
import com.emarsys.rdb.connector.bigquery.stream.parser.Parser
import com.emarsys.rdb.connector.bigquery.stream.sendrequest.SendRequestWithOauthHandling
import spray.json.JsObject


object BigQueryStreamSource {
  def apply[T](httpRequest: HttpRequest, parserFn: JsObject => T, tokenActor: ActorRef, http: HttpExt)
              (implicit tokenRequest: Timeout, mat: ActorMaterializer): Source[T, NotUsed] = Source.fromGraph(GraphDSL.create() {
    implicit builder =>
      import GraphDSL.Implicits._

      implicit val system = mat.system
      implicit val ec = mat.executionContext

      val in = builder.add(Source.repeat(httpRequest))
      val requestSender = builder.add(SendRequestWithOauthHandling(tokenActor, http))
      val parser = builder.add(Parser(parserFn))
      val pageTokenGenerator = builder.add(PageTokenGenerator[String]())
      val zip = builder.add(Zip[HttpRequest, Option[String]]())
      val addPageToken = builder.add(Flow[(HttpRequest, Option[String])].map { case (request, pageToken) =>
        request.copy(uri = request.uri.withQuery(Uri.Query(pageToken.map(token => s"pageToken=$token"))))
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
