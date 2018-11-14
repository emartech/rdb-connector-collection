package com.emarsys.rdb.connector.common

package object models {

  case class CommonConnectionReadableData(`type`: String, location: String, dataset: String, user: String)

  trait ConnectionConfig {
    def toCommonFormat: CommonConnectionReadableData

    final override def toString: String = {
      val CommonConnectionReadableData(t, location, dataset, user) = toCommonFormat
      s"""{"type":"$t","location":"$location","dataset":"$dataset","user":"$user"}"""
    }
  }

  case class MetaData(nameQuoter: String, valueQuoter: String, escape: String)

}
