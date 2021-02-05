package com.emarsys.rdb.connector.redshift

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.emarsys.rdb.connector.redshift.utils.{SelectDbInitHelper, SelectDbWithSchemaInitHelper}
import com.emarsys.rdb.connector.test.DeleteItSpec

import scala.concurrent.duration._

class RedshiftDeleteItSpec
    extends TestKit(ActorSystem("RedshiftDeleteItSpec"))
    with DeleteItSpec
    with SelectDbInitHelper {
  val aTableName: String = tableName
  val bTableName: String = s"temp_$uuid"

  override val awaitTimeout = 15.seconds



  override def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }

}

class RedshiftDeleteWithCurrentSchemaItSpec
    extends TestKit(ActorSystem("RedshiftDeleteWithCurrentSchemaItSpec"))
    with DeleteItSpec
    with SelectDbWithSchemaInitHelper {
  val aTableName: String = tableName
  val bTableName: String = s"temp_$uuid"

  override val awaitTimeout = 15.seconds



  override def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }

}
