package com.emarsys.rdb.connector.bigquery

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.http.scaladsl.model.{HttpEntity, HttpRequest, HttpResponse, StatusCodes}
import akka.stream.ActorMaterializer
import akka.testkit.{ImplicitSender, TestKit}
import com.emarsys.rdb.connector.bigquery.GoogleTokenActor.{TokenError, TokenRequest, TokenResponse}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import akka.pattern.ask
import akka.util.Timeout

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._

object GoogleTokenActorWithMockHttp {
  def props(clientEmail: String, privateKey: Array[Byte])(
    httpCallFunc: (HttpRequest) => Future[HttpResponse]
  )(implicit materializer: ActorMaterializer) = Props(new GoogleTokenActorWithMockHttp(clientEmail, privateKey)(httpCallFunc))
}

class GoogleTokenActorWithMockHttp(
                                    clientEmail: String, privateKey: Array[Byte]
                                  )(
                                    httpCallFunc: (HttpRequest) => Future[HttpResponse]
                                  )(implicit materializer: ActorMaterializer) extends GoogleTokenActor(clientEmail, privateKey) {

  override def httpCall(request: HttpRequest): Future[HttpResponse] = {
    httpCallFunc(request)
  }
}


class GoogleTokenActorSpec extends TestKit(ActorSystem("GoogleTokenActorSpec"))
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  implicit val materializer = ActorMaterializer()

  trait MockHttpCallContext {
    val privateKey = java.util.Base64.getDecoder.decode("MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDEgPjWN3OGtoS3\nENoG+mEt/nbMuBGgcwnnWu4wQreebUGIJ1qpvA0typWjz/EwQNwlzhTOfMQV680R\nbTChJJWR0H7TjEnXsodcbZvA+N4Tl5qtMA4eXxjHLWNni+dFCZB16wY3okYda6Dg\n+NCp1QaX5B8fhl5Cdzwgt/FfWD6pU45XD83TEGJBuMyTAsJhP17l+Hz2xxebzs3B\nlAiaDuZSACytsfyyLpGLR2sF0e21ZqySJbs5F8vomu9JNUZ/TivmzMrrIK367iCY\n/okFyhlJ6k8R9Fw21udEVFG/r4HCT8EwnYR9kfNfhXUHAqjQqtsKkinw2X7i7Usp\nUTjrDqs1AgMBAAECggEAJ34yRL9tSQhawP4yi497w4YuczOVW1Vzipt0Xp+yDrWv\no4EhUBa64VajX4J123hVpoV2Gg/qSuuS0etLiD91jhJEwxihaKf5W4Kt+Ikr/O0N\nybvsQn9jV/iPz2pHR1FGRuS+4aSMCfKtKTnomhF120YeWTQksqALJhpB+SMfqOEH\ndcULIPMhXkqqR9zYxnok1I/+FFoJ1/VcAOU14tOSXg79WkEUZj6An3EpYumtBgpX\nm4EAD+MvvefNyePLvi15tJGD2erv/tz4muFneKkylRNI8q447c+Jydx3nHe6FfTY\nMc6nAWynO2HOX7mLe/4UyVQh0syDBuct0Bmh1yIrCwKBgQDoq19PNZwAlU0V9kiS\nsRV7eG+aahq8AYadvQiX/l29OTL3xgBFL/i6pkfCJOW5tVxeLBP1mGZoYQ2ZAtSi\nrne0gZxt5nW7bf+aE46eDhNKsvDeY5FaSbos4SP2ZEr6UARzCURB5CEnJN4TjHrE\nM0euY8ZvbVYftDVPCxkEp9JqgwKBgQDYNTo/n5jlOhkeeiX5oW7K21xYRprDuIos\nVuP0idJQfa08f+/cMoPqgaWCc87slqRXtiSsD9yIsumnXhF3FaV2BjEQigz+WhuF\nLF85rAv9ZVPz5MZaHbFhtBEB3ZtO4mlZqqGnHtEAoKwRz22t/U1kHRgR4SOdT56i\n32dd1LQF5wKBgFtfGZHYwsfz2g765is9ges4M9PXQWJ90ujVWK+gBB4QfXSSfH6v\nRSW/sUSMCu9wSrLs6nWzgNwS6S0i0HCGxZnMoKsEK04M96kBbyug6XCXb0JWpblo\nZMXFMMNNRaihje3DQNwDhAWEU/YnX/r3DHpu0nnl3UGcGqdM+2k5oseTAoGBAI9P\nOLzTXNUUHXJGJMXCa12q6RraMdtphqy9K3v7npwbsahYZPTfxvC53qsJeC756xT4\ndnZWTSeO77EweQMmJfaFRCBiYRp3P6aWMshXcdsUPwF6sr8oz1qjsGI8MaWoDYyR\nvXS4yHBSD7v+cgTR0Wp6nmm7gY/UJqJu0mUvh+QhAoGBAMIoQXqFWSAvlKYaFkMx\ntgtI1/2MWbXX7c2z70qr9MVUDoDJOlBwrBeFsFbOKjNY1j4EzKi8cB+0yyk5cpRQ\nvTtX5Hjk9HT0iyr74PHW2+XDsvoIhmzzN00ITab+xZkXwdHEMpgOR0NdsRQnOf+H\nNJbK3HeyB+AD/9jVvfpKAbql".filterNot(_ == '\n'))

    class HttpCallMockActor extends Actor {
      val buffer = new scala.collection.mutable.ArrayBuffer[ActorRef]()

      override def receive: Receive = {
        case _: HttpRequest =>
          buffer += sender()
        case response: HttpResponse =>
          buffer.foreach(_ ! response)
          buffer.clear()
      }
    }

    private val httpCallMockActor = system.actorOf(Props(new HttpCallMockActor))

    def httpMockResponse(response: HttpResponse): Unit = {
      httpCallMockActor ! response
    }

    def httpMockResponseToken(token: String): Unit = {
      httpMockResponse(HttpResponse(entity = HttpEntity(s"""{"access_token":"$token"}""")))
    }

    private def httpMockCall(request: HttpRequest): Future[HttpResponse] = {
      implicit val timeout = Timeout(1.minutes)
      (httpCallMockActor ? request).mapTo[HttpResponse]
    }

    val tokenActor = system.actorOf(GoogleTokenActorWithMockHttp.props("clientemail", privateKey)(httpMockCall))

  }


  "GoogleTokenActor" must {

    "first msg - call google and send back token" in new MockHttpCallContext {
      tokenActor ! TokenRequest(false)
      expectNoMessage(500.milliseconds)

      httpMockResponseToken("TOKEN")
      expectMsg(TokenResponse("TOKEN"))
    }

    "first msg with force - call google and send back token" in new MockHttpCallContext {
      tokenActor ! TokenRequest(true)
      expectNoMessage(500.milliseconds)

      httpMockResponseToken("TOKEN")
      expectMsg(TokenResponse("TOKEN"))
    }

    "only first msg call google and send back token" in new MockHttpCallContext {
      tokenActor ! TokenRequest(false)
      expectNoMessage(500.milliseconds)

      httpMockResponseToken("TOKEN")
      expectMsg(TokenResponse("TOKEN"))

      tokenActor ! TokenRequest(false)
      expectMsg(TokenResponse("TOKEN"))
    }

    "force msg call google and send back token" in new MockHttpCallContext {
      tokenActor ! TokenRequest(false)
      expectNoMessage(500.milliseconds)

      httpMockResponseToken("TOKEN")
      expectMsg(TokenResponse("TOKEN"))

      tokenActor ! TokenRequest(false)
      expectMsg(TokenResponse("TOKEN"))

      tokenActor ! TokenRequest(true)
      expectNoMessage(500.milliseconds)

      httpMockResponseToken("TOKEN2")
      expectMsg(TokenResponse("TOKEN2"))
    }

    "send error - call google and send back error" in new MockHttpCallContext {
      tokenActor ! TokenRequest(false)
      expectNoMessage(500.milliseconds)

      httpMockResponse(HttpResponse(status = StatusCodes.BadRequest))
      expectMsg(TokenError)
    }


    "send error - after google send back error" in new MockHttpCallContext {
      tokenActor ! TokenRequest(true)
      expectNoMessage(500.milliseconds)

      httpMockResponse(HttpResponse(status = StatusCodes.BadRequest))
      expectMsg(TokenError)

      tokenActor ! TokenRequest(true)
      expectMsg(TokenError)

      tokenActor ! TokenRequest(false)
      expectMsg(TokenError)
    }

    "send multiple answer" in new MockHttpCallContext {
      tokenActor ! TokenRequest(false)
      tokenActor ! TokenRequest(true)
      tokenActor ! TokenRequest(true)
      tokenActor ! TokenRequest(false)
      expectNoMessage(500.milliseconds)

      httpMockResponseToken("TOKEN")
      expectMsg(TokenResponse("TOKEN"))
      expectMsg(TokenResponse("TOKEN"))
      expectMsg(TokenResponse("TOKEN"))
      expectMsg(TokenResponse("TOKEN"))
    }

  }
}
