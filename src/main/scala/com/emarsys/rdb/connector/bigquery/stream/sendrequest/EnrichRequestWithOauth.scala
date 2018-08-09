package com.emarsys.rdb.connector.bigquery.stream.sendrequest

import akka.NotUsed
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.stream.Materializer
import akka.stream.scaladsl.Flow
import com.emarsys.rdb.connector.bigquery.GoogleSession

import scala.concurrent.ExecutionContext

object EnrichRequestWithOauth {

  case class TokenErrorException() extends Exception

  def apply(googleSession: GoogleSession)(implicit ec: ExecutionContext, materializer: Materializer): Flow[HttpRequest, HttpRequest, NotUsed] = {
    Flow[HttpRequest].mapAsync(1) { request =>
        googleSession.getToken.map {
          token => request.addHeader(Authorization(OAuth2BearerToken(token)))
        }
    }
  }
}
