package com.emarsys.rbd.connector.bigquery

import akka.actor.ActorSystem
import akka.testkit.TestKit
import akka.util.Timeout
import com.emarsys.rbd.connector.bigquery.utils.MetaDbInitHelper
import com.emarsys.rdb.connector.test.MetadataItSpec
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration._

class BigQueryMetadataItSpec
    extends TestKit(ActorSystem("BigQueryMetadataItSpec"))
    with MetadataItSpec
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with MetaDbInitHelper {

  implicit override val sys: ActorSystem                = system

  implicit override val timeout: Timeout                = Timeout(30.second)

  override val awaitTimeout = 30.seconds

  override def beforeAll(): Unit = {
    initDb()
  }

  override def afterAll(): Unit = {
    cleanUpDb()
    shutdown()
  }

}
