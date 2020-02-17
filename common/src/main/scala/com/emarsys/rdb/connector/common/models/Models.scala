package com.emarsys.rdb.connector.common

import java.security.MessageDigest
import java.math.BigInteger

object Models {

  case class CommonConnectionReadableData(`type`: String, location: String, dataset: String, user: String)

  trait ConnectionConfig {
    def replica[C <: this.type]: Option[C] = None

    protected def getPublicFieldsForId: List[String]

    protected def getSecretFieldsForId: List[String]

    def getId: String = {
      val publicPart = getPublicFieldsForId.mkString("|")
      val secretPart = sha256Hash(getSecretFieldsForId.mkString("|"))
      s"$publicPart|$secretPart"
    }

    private def sha256Hash(text: String): String = {
      val bytes: Array[Byte] = MessageDigest.getInstance("SHA-256").digest(text.getBytes("UTF-8"))
      String.format("%064x", new BigInteger(1, bytes))
    }

    def toCommonFormat: CommonConnectionReadableData

    final override def toString: String = {
      val CommonConnectionReadableData(t, location, dataset, user) = toCommonFormat
      s"""{"type":"$t","location":"$location","dataset":"$dataset","user":"$user"}"""
    }
  }

  case class PoolConfig(maxPoolSize: Int, queueSize: Int)
  case class ConnectorConfig(
      select: PoolConfig,
      update: PoolConfig,
      segment: PoolConfig,
      meta: PoolConfig,
      test: PoolConfig
  )

  case class MetaData(nameQuoter: String, valueQuoter: String, escape: String)

}
