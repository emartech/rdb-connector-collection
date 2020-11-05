package com.emarsys.rdb.connector.bigquery

import java.time.Clock

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.http.scaladsl.model._
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.http.scaladsl.{HttpExt, HttpsConnectionContext}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.bigquery.GoogleTokenApi.AccessTokenExpiry
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{verify, when}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import org.scalatestplus.mockito.MockitoSugar
import pdi.jwt.{Jwt, JwtAlgorithm}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class GoogleTokenApiSpec
    extends TestKit(ActorSystem("GoogleTokenApiSpec"))
    with WordSpecLike
    with Matchers
    with ScalaFutures
    with MockitoSugar
    with BeforeAndAfterAll {

  override def afterAll(): Unit =
    shutdown()
  implicit val defaultPatience =
    PatienceConfig(timeout = 2.seconds, interval = 50.millis)

  implicit val executionContext: ExecutionContext = system.dispatcher

  //http://travistidwell.com/jsencrypt/demo/
  val privateKey =
    """-----BEGIN PRIVATE KEY-----
      |MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCpq9CbIxotQFod
      |jKsQhGe4/j14iEEj2KdU1oviHEpFzvVk1BDiB8BQcC11+8TnR/wZHtF2j0OMyits
      |0tHz4uWjiFZ1KR8EuSbj0rixwsWHj7L08wtktuM5RTFc+mnQB9UyFEHAaISmrEse
      |ehEZnSZAPY2SIw2ZeWa+CFZPKAUBb98KeMNHNvTWZAC7/3xCKVgwOL7E1sLpPSAG
      |zLx/okpM5pheWi9zbhGpL6JUqhS9I60PpHIkfoe7cmE6UTGt2hZv07EnE+n8qWEV
      |zQkKe+3ZGNAL++YBrNQGOhlyG2Wus++U6dPjW3XpZSQnMjNfOjdfPtK/DjEmui1H
      |5zUJYyYLAgMBAAECggEBAIN2BklNZ0jMZYYjqZ9Al4T0J/ityZrWkL/hA70Lolh4
      |RBX9YZ9f1hf88pxJmISCd8eW40BzPClns0G8DsRidv6/8g0Q8WGde45lhIjmmlmw
      |cz3q2lcMhP2oqNibhUST7RHCNDe6Q51IPWO9vGYWJr77cidaVX0mXP68QvNN1KMJ
      |IddAyzoVFHvx64WjtvwIscGmt5vEFR1qfyBVkqqszyV8lxA/Yx2NDPyhDwlvJgQH
      |99Jn14G9iVqe687OpRRKAhyCKfjCP7S0oXzY4WDxJANG1zloiiu3xZo0zTx7fIhF
      |/M2CKNgbaus0THAMlIXZsYPoU/nHbGWDGINzYoK67FECgYEA0N1h0ABQVP8H4VX4
      |qRcpJRhmMahm0KjRad64JNUFkUpejwSzhS9+jkwlJHTC5kuiEedhqFlhTlciRM5O
      |V60nB4IiwVFxtig57C8dJE3MZtPp8z0GY0p0q+vzJkJkSboT4gibOtZ8Ex8ZOEt3
      |iDCqrFxBqUV/lg96YjbRtA2wSBcCgYEAz/YfmWRIEI4dN4Jsrmf+v6jvp6CRYJ+1
      |Q9xAf9QoQO29yWtIu+0ehJM1C0mnW64qlFv/vwGwUjSg56DN0zjghWHY36V+6MQw
      |lmZbhuol0+RJpAd53RRG0ieDpJot3Og4ac+giAnqu/t0phdvqurkUzGQRm7XfFKS
      |F2c246K/li0CgYAnmfITyBtzIi/ST8Sn+tY4TFoEDFQCOCAMnMf5Y9J8a8dmApQv
      |KoQqYtGgCetyUuDV2DMyAlBq1CaROTStdw0xZUFFujV/Pj1NRNmXqpCY5pEzJ1zw
      |sotQlZoypN/zAq8Gam9URpqb3Yegnt55GhEiQRPKDn1UbHbd7Frycq373QKBgBCn
      |9NXzSzZQO5TTlGLPn364SCGT4bDsebcqr9vNIUA6CmZUemnazwPtSmVSC95y76Qc
      |Tjp9JyMeZfjHT1TojEsCkD0xYx2/gOi63//JRyhWc3N4ydDkK9vvIEMRujSkQMhw
      |wbnDmZJezHP1EpOM5qanJJgPjqC9eEf5k1LeGRwtAoGBAIASfPZoZzKVggGFgbct
      |QhoeJsVvasj5qPsJte/3KhLIdlH6INeui7xqS4Ko3pL4we2Tgk8ZDA276xRVFLgd
      |3WUdpJTg8VdGsP/gVoI58KbuB7C6KviubQQsq9AYqJ4mfBbobLXbCS+3SQ0xfJzd
      |P82SRXe13AftvjVQmP2caarJ
      |-----END PRIVATE KEY-----""".stripMargin

  val publicKey =
    """-----BEGIN PUBLIC KEY-----
      |MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAqavQmyMaLUBaHYyrEIRn
      |uP49eIhBI9inVNaL4hxKRc71ZNQQ4gfAUHAtdfvE50f8GR7Rdo9DjMorbNLR8+Ll
      |o4hWdSkfBLkm49K4scLFh4+y9PMLZLbjOUUxXPpp0AfVMhRBwGiEpqxLHnoRGZ0m
      |QD2NkiMNmXlmvghWTygFAW/fCnjDRzb01mQAu/98QilYMDi+xNbC6T0gBsy8f6JK
      |TOaYXlovc24RqS+iVKoUvSOtD6RyJH6Hu3JhOlExrdoWb9OxJxPp/KlhFc0JCnvt
      |2RjQC/vmAazUBjoZchtlrrPvlOnT41t16WUkJzIzXzo3Xz7Svw4xJrotR+c1CWMm
      |CwIDAQAB
      |-----END PUBLIC KEY-----""".stripMargin

  "GoogleTokenApi" should {

    "call the api as the docs want to" in {
      val http = mock[HttpExt]
      when(
        http.singleRequest(
          any[HttpRequest](),
          any[HttpsConnectionContext](),
          any[ConnectionPoolSettings](),
          any[LoggingAdapter]()
        )
      ).thenReturn(
        Future.successful(
          HttpResponse(
            entity = HttpEntity(
              ContentTypes.`application/json`,
              """{"access_token": "token", "token_type": "String", "expires_in": 3600}"""
            )
          )
        )
      )

      implicit val clock: Clock = java.time.Clock.systemUTC()
      val api                   = new GoogleTokenApi(http)
      Await.result(api.getAccessToken("test@example.com", privateKey), defaultPatience.timeout)

      val captor: ArgumentCaptor[HttpRequest] = ArgumentCaptor.forClass(classOf[HttpRequest])
      verify(http).singleRequest(
        captor.capture(),
        any[HttpsConnectionContext](),
        any[ConnectionPoolSettings](),
        any[LoggingAdapter]()
      )
      val request: HttpRequest = captor.getValue
      request.uri.toString shouldBe "https://www.googleapis.com/oauth2/v4/token"
      val data = Unmarshal(request.entity).to[String].futureValue
      data should startWith("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=")
      val jwt     = data.replace("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=", "")
      val decoded = Jwt.decode(jwt, publicKey, Seq(JwtAlgorithm.RS256))
      decoded.isSuccess shouldBe true
      decoded.get.content should include(""""aud":"https://www.googleapis.com/oauth2/v4/token"""")
      decoded.get.content should include(""""scope":"https://www.googleapis.com/auth/bigquery"""")
      decoded.get.content should include(""""iss":"test@example.com"""")

    }

    "return the token" in {
      val http = mock[HttpExt]
      when(
        http.singleRequest(
          any[HttpRequest](),
          any[HttpsConnectionContext](),
          any[ConnectionPoolSettings](),
          any[LoggingAdapter]()
        )
      ).thenReturn(
        Future.successful(
          HttpResponse(
            entity = HttpEntity(
              ContentTypes.`application/json`,
              """{"access_token": "token", "token_type": "String", "expires_in": 3600}"""
            )
          )
        )
      )

      implicit val clock: Clock = java.time.Clock.systemUTC()

      val api = new GoogleTokenApi(http)
      api.getAccessToken("test@example.com", privateKey).futureValue should matchPattern {
        case AccessTokenExpiry("token", exp) if exp > (System.currentTimeMillis / 1000L + 3000L) =>
      }
    }
  }

}
