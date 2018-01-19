package com.emarsys.rdb.connector.bigquery.util

import java.time.Instant

import com.emarsys.rdb.connector.bigquery.GoogleApi._
import io.igl.jwt._

object GoogleJwt {

  def create(clientEmail: String, privateKey: Array[Byte]): String = {
    val time = Instant.now.getEpochSecond
    val jwt = new DecodedJwt(Seq(Alg(Algorithm.RS256), Typ("JWT")),
      Seq(
        Iss(clientEmail),
        Scope(bigQueryAuthUrl),
        Aud(googleTokenUrl),
        Exp(time + 3600),
        Iat(time)
      )
    )
    jwt.encodedAndSigned(privateKey)
  }
}
