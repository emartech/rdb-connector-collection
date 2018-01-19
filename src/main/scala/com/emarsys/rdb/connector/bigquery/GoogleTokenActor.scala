package com.emarsys.rdb.connector.bigquery

import java.util.Base64

import akka.actor.{ActorRef, FSM, Props}
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model.{FormData, HttpMethods, HttpRequest, HttpResponse}
import akka.stream.ActorMaterializer
import com.emarsys.rdb.connector.bigquery.GoogleApi.googleTokenUrl
import com.emarsys.rdb.connector.bigquery.GoogleTokenActor._
import com.emarsys.rdb.connector.bigquery.util.AkkaHttpPimps._
import com.emarsys.rdb.connector.bigquery.util.GoogleJwt
import spray.json.{DefaultJsonProtocol, JsonFormat}

import scala.concurrent.Future
import scala.util.Success

object GoogleTokenActor {
  def props(clientEmail: String, privateKey: String, http: HttpExt)(implicit materializer: ActorMaterializer) = Props(new GoogleTokenActor(clientEmail, privateKey, http))

  sealed trait Message
  case class TokenRequest(force: Boolean) extends Message
  case class TokenResponse(token: String) extends Message
  case object TokenError extends Message
  private case class NewToken(token: String) extends Message

  sealed trait State
  case object Starting extends State
  case object InRefresh extends State
  case object Idle extends State
  case object Error extends State

  sealed trait Data
  case class SenderList(list: Seq[ActorRef]) extends Data
  case class Token(token: String) extends Data

  object TokenJsonProtocol extends DefaultJsonProtocol{
    case class TokenResponse(accessToken: String)
    implicit val tokenFormat: JsonFormat[TokenResponse] = jsonFormat(TokenResponse, "access_token")
  }
}

class GoogleTokenActor(clientEmail: String, privateKey: String, http: HttpExt)(implicit val materializer: ActorMaterializer) extends FSM[State, Data] {
  val decodedPrivateKey = Base64.getDecoder.decode(privateKey.replace("\n", ""))

  startWith(Starting, SenderList(Seq.empty))

  when(Starting) {
    case Event(TokenRequest(_), _) =>
      doRefresh()
      goto(InRefresh) using SenderList(Seq(sender()))
  }

  when(InRefresh) {
    case Event(TokenRequest(_), SenderList(list)) =>
      stay using SenderList(list :+ sender())

    case Event(NewToken(token), SenderList(list)) =>
      list.foreach(_ ! TokenResponse(token))
      goto(Idle) using Token(token)

    case Event(TokenError, SenderList(list)) =>
      list.foreach(_ ! TokenError)
      goto(Error)
  }

  when(Idle) {
    case Event(TokenRequest(false), Token(token)) =>
      sender() ! TokenResponse(token)
      stay

    case Event(TokenRequest(true), _) =>
      doRefresh()
      goto(InRefresh) using SenderList(Seq(sender()))
  }

  when(Error) {
    case _ =>
      sender() ! TokenError
      stay
  }

  whenUnhandled {
    case Event(e, s) =>
      log.warning("received unhandled request {} in state {}/{}", e, stateName, s)
      stay
  }

  initialize()

  import context.dispatcher

  def doRefresh(): Unit = {
    val jwt = GoogleJwt.create(clientEmail, decodedPrivateKey)
    val request = createTokenRequest(jwt)
    val response = http.singleRequest(request)
    handleResponse(response)
  }

  def handleResponse(responseF: Future[HttpResponse]): Unit = {
    responseF.onComplete {
      case Success(response) if response.status.isSuccess() =>
        parseToken(response).foreach {
          case Some(token) => self ! NewToken(token)
          case _ => self ! TokenError
        }
      case _ => self ! TokenError
    }
  }

  def createTokenRequest(jwt: String): HttpRequest = {
    val requestEntity = FormData(
      "grant_type" -> "urn:ietf:params:oauth:grant-type:jwt-bearer",
      "assertion" -> jwt
    ).toEntity

    HttpRequest(HttpMethods.POST, googleTokenUrl, entity = requestEntity)
  }

  def parseToken(response: HttpResponse): Future[Option[String]] = {
    import spray.json._

    response.entity.convertToString()
      .map(body => Option(body.parseJson.convertTo[TokenJsonProtocol.TokenResponse].accessToken))
      .recover {
        case _ => None
      }
  }
}
