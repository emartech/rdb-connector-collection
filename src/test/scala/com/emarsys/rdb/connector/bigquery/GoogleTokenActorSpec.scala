package com.emarsys.rdb.connector.bigquery

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.http.scaladsl.{HttpExt, HttpsConnectionContext}
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.{ImplicitSender, TestKit}
import com.emarsys.rdb.connector.bigquery.GoogleTokenActor.{TokenError, TokenRequest, TokenResponse}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Future

class GoogleTokenActorSpec extends TestKit(ActorSystem("GoogleTokenActorSpec"))
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  implicit val materializer = ActorMaterializer()

  val mockHttp =
    new HttpExt(null) {
      var response: Future[HttpResponse] = Future.failed(new Exception())

      override def singleRequest(request: HttpRequest, connectionContext: HttpsConnectionContext, settings: ConnectionPoolSettings, log: LoggingAdapter)(implicit fm: Materializer): Future[HttpResponse] =
        response
    }

  trait MockHttpScope {
    val privateKey = "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDEgPjWN3OGtoS3\nENoG+mEt/nbMuBGgcwnnWu4wQreebUGIJ1qpvA0typWjz/EwQNwlzhTOfMQV680R\nbTChJJWR0H7TjEnXsodcbZvA+N4Tl5qtMA4eXxjHLWNni+dFCZB16wY3okYda6Dg\n+NCp1QaX5B8fhl5Cdzwgt/FfWD6pU45XD83TEGJBuMyTAsJhP17l+Hz2xxebzs3B\nlAiaDuZSACytsfyyLpGLR2sF0e21ZqySJbs5F8vomu9JNUZ/TivmzMrrIK367iCY\n/okFyhlJ6k8R9Fw21udEVFG/r4HCT8EwnYR9kfNfhXUHAqjQqtsKkinw2X7i7Usp\nUTjrDqs1AgMBAAECggEAJ34yRL9tSQhawP4yi497w4YuczOVW1Vzipt0Xp+yDrWv\no4EhUBa64VajX4J123hVpoV2Gg/qSuuS0etLiD91jhJEwxihaKf5W4Kt+Ikr/O0N\nybvsQn9jV/iPz2pHR1FGRuS+4aSMCfKtKTnomhF120YeWTQksqALJhpB+SMfqOEH\ndcULIPMhXkqqR9zYxnok1I/+FFoJ1/VcAOU14tOSXg79WkEUZj6An3EpYumtBgpX\nm4EAD+MvvefNyePLvi15tJGD2erv/tz4muFneKkylRNI8q447c+Jydx3nHe6FfTY\nMc6nAWynO2HOX7mLe/4UyVQh0syDBuct0Bmh1yIrCwKBgQDoq19PNZwAlU0V9kiS\nsRV7eG+aahq8AYadvQiX/l29OTL3xgBFL/i6pkfCJOW5tVxeLBP1mGZoYQ2ZAtSi\nrne0gZxt5nW7bf+aE46eDhNKsvDeY5FaSbos4SP2ZEr6UARzCURB5CEnJN4TjHrE\nM0euY8ZvbVYftDVPCxkEp9JqgwKBgQDYNTo/n5jlOhkeeiX5oW7K21xYRprDuIos\nVuP0idJQfa08f+/cMoPqgaWCc87slqRXtiSsD9yIsumnXhF3FaV2BjEQigz+WhuF\nLF85rAv9ZVPz5MZaHbFhtBEB3ZtO4mlZqqGnHtEAoKwRz22t/U1kHRgR4SOdT56i\n32dd1LQF5wKBgFtfGZHYwsfz2g765is9ges4M9PXQWJ90ujVWK+gBB4QfXSSfH6v\nRSW/sUSMCu9wSrLs6nWzgNwS6S0i0HCGxZnMoKsEK04M96kBbyug6XCXb0JWpblo\nZMXFMMNNRaihje3DQNwDhAWEU/YnX/r3DHpu0nnl3UGcGqdM+2k5oseTAoGBAI9P\nOLzTXNUUHXJGJMXCa12q6RraMdtphqy9K3v7npwbsahYZPTfxvC53qsJeC756xT4\ndnZWTSeO77EweQMmJfaFRCBiYRp3P6aWMshXcdsUPwF6sr8oz1qjsGI8MaWoDYyR\nvXS4yHBSD7v+cgTR0Wp6nmm7gY/UJqJu0mUvh+QhAoGBAMIoQXqFWSAvlKYaFkMx\ntgtI1/2MWbXX7c2z70qr9MVUDoDJOlBwrBeFsFbOKjNY1j4EzKi8cB+0yyk5cpRQ\nvTtX5Hjk9HT0iyr74PHW2+XDsvoIhmzzN00ITab+xZkXwdHEMpgOR0NdsRQnOf+H\nNJbK3HeyB+AD/9jVvfpKAbql".filterNot(_ == '\n')
    val clientEmail = "clientEmail"

    def createTokenActor() = {
      system.actorOf(GoogleTokenActor.props(clientEmail, privateKey, mockHttp))
    }

    def setSuccessResponse(responseToken: String) = {
      mockHttp.response = Future.successful(HttpResponse(entity = s"""{"access_token": "$responseToken"}"""))
    }

    def setFailResponse() = {
      mockHttp.response = Future.failed(new Exception())
    }
  }


  "GoogleTokenActor" must {

    "first msg - call google and send back token" in new MockHttpScope {
      val tokenActor = createTokenActor()
      setSuccessResponse("TOKEN")

      tokenActor ! TokenRequest(false)
      expectMsg(TokenResponse("TOKEN"))
    }

    "first msg with force - call google and send back token" in new MockHttpScope {
      val tokenActor = createTokenActor()
      setSuccessResponse("TOKEN")

      tokenActor ! TokenRequest(true)
      expectMsg(TokenResponse("TOKEN"))
    }

    "only first msg call google and send back token" in new MockHttpScope {
      val tokenActor = createTokenActor()
      setSuccessResponse("TOKEN")

      tokenActor ! TokenRequest(false)
      expectMsg(TokenResponse("TOKEN"))

      tokenActor ! TokenRequest(false)
      expectMsg(TokenResponse("TOKEN"))
    }

    "force msg call google and send back token" in new MockHttpScope {
      val tokenActor = createTokenActor()
      setSuccessResponse("TOKEN")

      tokenActor ! TokenRequest(false)
      expectMsg(TokenResponse("TOKEN"))

      tokenActor ! TokenRequest(false)
      expectMsg(TokenResponse("TOKEN"))

      setSuccessResponse("TOKEN2")
      tokenActor ! TokenRequest(true)
      expectMsg(TokenResponse("TOKEN2"))
    }

    "send error - call google and send back error" in new MockHttpScope {
      val tokenActor = createTokenActor()
      setFailResponse()

      tokenActor ! TokenRequest(false)
      expectMsg(TokenError)
    }


    "send error - after google send back error" in new MockHttpScope {
      val tokenActor = createTokenActor()
      setFailResponse()

      tokenActor ! TokenRequest(true)
      expectMsg(TokenError)

      tokenActor ! TokenRequest(true)
      expectMsg(TokenError)

      tokenActor ! TokenRequest(false)
      expectMsg(TokenError)
    }

    "send multiple answer" in new MockHttpScope {
      val tokenActor = createTokenActor()
      setSuccessResponse("TOKEN")

      tokenActor ! TokenRequest(false)
      tokenActor ! TokenRequest(true)
      tokenActor ! TokenRequest(true)
      tokenActor ! TokenRequest(false)

      expectMsg(TokenResponse("TOKEN"))
      expectMsg(TokenResponse("TOKEN"))
      expectMsg(TokenResponse("TOKEN"))
      expectMsg(TokenResponse("TOKEN"))
    }

  }
}
