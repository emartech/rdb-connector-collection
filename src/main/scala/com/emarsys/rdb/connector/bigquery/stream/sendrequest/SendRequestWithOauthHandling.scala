package com.emarsys.rdb.connector.bigquery.stream.sendrequest

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.scaladsl.{Flow, GraphDSL}
import akka.stream.{ActorMaterializer, FlowShape, Graph, Materializer}
import com.emarsys.rdb.connector.bigquery.GoogleSession
import com.emarsys.rdb.connector.common.models.Errors._

import scala.concurrent.{ExecutionContext, Future}

object SendRequestWithOauthHandling {
  def apply(googleSession: GoogleSession, http: HttpExt)(
      implicit ec: ExecutionContext,
      system: ActorSystem,
      mat: ActorMaterializer
  ): Graph[FlowShape[HttpRequest, HttpResponse], NotUsed] = {
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val addGoogleOauthToken  = builder.add(EnrichRequestWithOauth(googleSession))
      val sendHttpRequest      = builder.add(Flow[HttpRequest].mapAsync(1)(http.singleRequest(_)))
      val responseErrorHandler = builder.add(Flow[HttpResponse].mapAsync(1)(handleRequestError(_)))

      addGoogleOauthToken ~> sendHttpRequest ~> responseErrorHandler

      FlowShape[HttpRequest, HttpResponse](addGoogleOauthToken.in, responseErrorHandler.out)
    }
  }

  private def handleRequestError(response: HttpResponse)(implicit materializer: Materializer) = {
    import com.emarsys.rdb.connector.bigquery.util.AkkaHttpPimps._
    implicit val ec: ExecutionContext = materializer.executionContext
    if (response.status.isFailure) {
      response.entity.convertToString().flatMap(body => Future.failed(errorFor(response, body)))
    } else {
      Future.successful(response)
    }
  }

  import errors._
  private def errorFor(response: HttpResponse, body: String): ConnectorError = (response, body) match {
    case SyntaxError(msg)   => SqlSyntaxError(msg)
    case NotFoundTable(msg) => TableNotFound(msg)
    case _                  => ConnectionError(new IllegalStateException(s"Unexpected error in response: ${response.status}}, $body"))

  }

  private object errors {
    object SyntaxError {
      def unapply(r: (HttpResponse, String)): Option[String] = {
        val (response, body) = r
        if (response.status == BadRequest && body.contains("Syntax error")) errorMessageFrom(body)
        else None
      }
    }

    object NotFoundTable {
      def unapply(r: (HttpResponse, String)): Option[String] = {
        val (response, body) = r
        if (response.status == NotFound && body.contains("Not found: Table")) errorMessageFrom(body)
        else None
      }
    }

    import spray.json._
    import DefaultJsonProtocol._

    private case class ErrorBody(error: Option[ErrorMessage])
    private case class ErrorMessage(message: String)
    implicit private val msgFormat  = jsonFormat1(ErrorMessage)
    implicit private val bodyFormat = jsonFormat1(ErrorBody)

    private def errorMessageFrom(body: String): Option[String] =
      body.parseJson.convertTo[ErrorBody].error.map(_.message)
  }
}
