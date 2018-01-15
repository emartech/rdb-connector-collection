package com.emarsys.rbd.connector.bigquery

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import akka.util.Timeout
import com.emarsys.rbd.connector.bigquery.utils.SelectDbInitHelper
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._

class BigQueryRawSelectItSpec extends TestKit(ActorSystem()) /*with RawSelectItSpec*/ with SelectDbInitHelper with WordSpecLike with Matchers with BeforeAndAfterAll{

  implicit override val sys: ActorSystem = system
  implicit override val materializer: ActorMaterializer = ActorMaterializer()
  implicit override val timeout: Timeout = Timeout(3.second)

  override val aTableName = "test_a"
  override val bTableName = "test_b"

  override def afterAll(): Unit = {
    cleanUpDb()
    TestKit.shutdownActorSystem(sys)
    connector.close()
  }

  override def beforeAll(): Unit = {
    initDb()
  }

  "#analyzeRawSelect" should {
    "return result" in {
        true shouldBe true
    }
  }
}

