package com.emarsys.rdb.connector.redshift

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.redshift.utils.{SelectDbInitHelper, SelectDbWithSchemaInitHelper}
import com.emarsys.rdb.connector.test.UpdateItSpec

import scala.concurrent.duration._

class RedshiftUpdateItSpec
    extends TestKit(ActorSystem("RedshiftUpdateItSpec"))
    with UpdateItSpec
    with SelectDbInitHelper {
  val aTableName: String = tableName
  val bTableName: String = s"temp_$uuid"

  override val awaitTimeout = 15.seconds

  implicit override val materializer: Materializer = ActorMaterializer()

  override def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }
}

class RedshiftUpdateWithSchemaItSpec
    extends TestKit(ActorSystem("RedshiftUpdateWithSchemaItSpec"))
    with UpdateItSpec
    with SelectDbWithSchemaInitHelper {
  val aTableName: String = tableName
  val bTableName: String = s"temp_$uuid"

  override val awaitTimeout = 15.seconds

  implicit override val materializer: Materializer = ActorMaterializer()

  override def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }
}
