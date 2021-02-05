package com.emarsys.rdb.connector.redshift

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.emarsys.rdb.connector.redshift.utils.{SelectDbInitHelper, SelectDbWithSchemaInitHelper}
import com.emarsys.rdb.connector.test.ReplaceItSpec

import scala.concurrent.duration._

class RedshiftReplaceItSpec
    extends TestKit(ActorSystem("RedshiftReplaceItSpec"))
    with ReplaceItSpec
    with SelectDbInitHelper {
  val aTableName: String = tableName
  val bTableName: String = s"temp_$uuid"

  override val awaitTimeout = 15.seconds



  override def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }

}

class RedshiftReplaceWithSchemaItSpec
    extends TestKit(ActorSystem("RedshiftReplaceWithSchemaItSpec"))
    with ReplaceItSpec
    with SelectDbWithSchemaInitHelper {
  val aTableName: String = tableName
  val bTableName: String = s"temp_$uuid"

  override val awaitTimeout = 15.seconds



  override def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }

}
