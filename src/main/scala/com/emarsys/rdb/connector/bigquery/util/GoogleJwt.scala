package com.emarsys.rdb.connector.bigquery.util

import java.time.Instant

import io.igl.jwt._

object GoogleJwt {

  def create(clientEmail: String, privateKey: Array[Byte]): String = {
    val time = Instant.now.getEpochSecond
    val jwt = new DecodedJwt(Seq(Alg(Algorithm.RS256), Typ("JWT")),
      Seq(
        Iss(clientEmail),
        Scope("https://www.googleapis.com/auth/bigquery"),
        Aud("https://www.googleapis.com/oauth2/v4/token"),
        Exp(time + 3600),
        Iat(time)
      )
    )
    jwt.encodedAndSigned(privateKey)
  }
}
