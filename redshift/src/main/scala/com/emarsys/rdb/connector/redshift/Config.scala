package com.emarsys.rdb.connector.redshift

import com.typesafe.config.ConfigFactory

import scala.util.Try

trait Config {

  private val config = ConfigFactory.load()

  object db {
    lazy val useHikari = Try(config.getBoolean("redshiftdb.use-hikari")).getOrElse(false)
  }

}

object Config extends Config
