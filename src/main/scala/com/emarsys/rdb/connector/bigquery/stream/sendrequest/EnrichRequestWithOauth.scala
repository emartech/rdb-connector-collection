package com.emarsys.rdb.connector.bigquery.stream.sendrequest

import akka.NotUsed
import akka.actor.ActorRef
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.pattern.ask
import akka.stream.scaladsl.Flow
import akka.util.Timeout
import com.emarsys.rdb.connector.bigquery.GoogleTokenActor.{TokenError, TokenRequest, TokenResponse}

import scala.concurrent.ExecutionContext

object EnrichRequestWithOauth {

  case object TokenErrorException extends Exception

  def apply(tokenActor: ActorRef)(implicit to: Timeout, ec: ExecutionContext): Flow[(HttpRequest, Boolean), HttpRequest, NotUsed] = {
    Flow[(HttpRequest, Boolean)].mapAsync(1) {
      case (request, force) =>
        (tokenActor ? TokenRequest(force)).map {
          case TokenResponse(token) => request.addHeader(Authorization(OAuth2BearerToken(token)))
          case TokenError           => throw TokenErrorException
        }
    }
  }
}
