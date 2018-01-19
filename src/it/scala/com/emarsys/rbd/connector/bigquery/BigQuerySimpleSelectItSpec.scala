package com.emarsys.rbd.connector.bigquery

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import akka.util.Timeout
import com.emarsys.rbd.connector.bigquery.utils.SelectDbInitHelper
import com.emarsys.rdb.connector.test.SimpleSelectItSpec
import org.scalatest._

import scala.concurrent.duration._

class BigQuerySimpleSelectItSpec extends TestKit(ActorSystem()) with SimpleSelectItSpec with WordSpecLike with Matchers with BeforeAndAfterAll with SelectDbInitHelper {

  override implicit val sys: ActorSystem = system
  override implicit val materializer: ActorMaterializer = ActorMaterializer()
  override implicit val timeout: Timeout = 5.seconds

  override def afterAll(): Unit = {
    cleanUpDb()
    connector.close()
    shutdown()
  }

  override def beforeAll(): Unit = {
    initDb()
  }
}
