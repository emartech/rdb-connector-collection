package com.emarsys.rdb.connector.bigquery.util

import scala.util.Try

object PrivateKeyParser {

  private val privateKeyStart = "-----BEGIN PRIVATE KEY-----"
  private val privateKeyStop = "-----END PRIVATE KEY-----"

  def parse(key: String): Option[Array[Byte]] = {
    val oneLineKey = key.filterNot(_ == '\n')
    if (!oneLineKey.startsWith(privateKeyStart) || !oneLineKey.endsWith(privateKeyStop)) {
      None
    } else {
      val encodedKey = oneLineKey.drop(privateKeyStart.length).dropRight(privateKeyStop.length)
      Try(java.util.Base64.getDecoder.decode(encodedKey)).toOption
    }
  }
}
