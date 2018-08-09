package com.emarsys.rdb.connector.bigquery.stream.sendrequest

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.scaladsl.{Flow, GraphDSL}
import akka.stream.{ActorMaterializer, FlowShape, Graph, Materializer}
import com.emarsys.rdb.connector.bigquery.GoogleSession

import scala.concurrent.{ExecutionContext, Future}

object SendRequestWithOauthHandling {
  def apply(googleSession: GoogleSession, http: HttpExt)(
    implicit ec: ExecutionContext,
    system: ActorSystem,
    mat: ActorMaterializer
  ): Graph[FlowShape[HttpRequest, HttpResponse], NotUsed] = {
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val addGoogleOauthToken   = builder.add(EnrichRequestWithOauth(googleSession))
      val sendHttpRequest       = builder.add(Flow[HttpRequest].mapAsync(1)(http.singleRequest(_)))
      val responseErrorHandler = builder.add(Flow[HttpResponse].mapAsync(1)(handleRequestError(_)))

      addGoogleOauthToken ~> sendHttpRequest ~> responseErrorHandler

      FlowShape[HttpRequest, HttpResponse](addGoogleOauthToken.in, responseErrorHandler.out)
    }
  }

  private def handleRequestError(response: HttpResponse)(implicit materializer: Materializer) = {
    import com.emarsys.rdb.connector.bigquery.util.AkkaHttpPimps._
    implicit val ec: ExecutionContext = materializer.executionContext
    if(response.status.isFailure) {
      response.entity.convertToString().map(errorBody => throw new IllegalStateException(s"Unexpected error in response: ${response.status}}, $errorBody"))
    } else {
      Future.successful(response)
    }
  }
}
