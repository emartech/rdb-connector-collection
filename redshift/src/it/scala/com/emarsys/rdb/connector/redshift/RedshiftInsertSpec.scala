package com.emarsys.rdb.connector.redshift

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.emarsys.rdb.connector.redshift.utils.{SelectDbInitHelper, SelectDbWithSchemaInitHelper}
import com.emarsys.rdb.connector.test.InsertItSpec

import scala.concurrent.duration._

class RedshiftInsertSpec extends TestKit(ActorSystem("RedshiftInsertSpec")) with InsertItSpec with SelectDbInitHelper {

  val aTableName: String = tableName
  val bTableName: String = s"temp_$uuid"

  override val awaitTimeout = 15.seconds



  override def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }
}

class RedshiftInsertWithSchemaSpec
    extends TestKit(ActorSystem("RedshiftInsertWithSchemaSpec"))
    with InsertItSpec
    with SelectDbWithSchemaInitHelper {

  val aTableName: String = tableName
  val bTableName: String = s"temp_$uuid"

  override val awaitTimeout = 15.seconds



  override def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }
}
