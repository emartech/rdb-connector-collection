package com.emarsys.rbd.connector.bigquery

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import akka.util.Timeout
import com.emarsys.rbd.connector.bigquery.utils.{SelectDbInitHelper, TestHelper}
import com.emarsys.rdb.connector.test.RawSelectItSpec
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

class BigQueryRawSelectItSpec extends TestKit(ActorSystem()) with RawSelectItSpec with SelectDbInitHelper with WordSpecLike with Matchers with BeforeAndAfterAll{

  implicit override val sys: ActorSystem = system
  implicit override val materializer: ActorMaterializer = ActorMaterializer()
  implicit override val timeout: Timeout = Timeout(30.second)
  override implicit val executionContext: ExecutionContextExecutor = sys.dispatcher

  override val awaitTimeout = 30.seconds

  override def afterAll(): Unit = {
    cleanUpDb()
    connector.close()
    shutdown()
  }

  override def beforeAll(): Unit = {
    initDb()
  }

  val dataset = TestHelper.TEST_CONNECTION_CONFIG.dataset

  val simpleSelect = s"SELECT * FROM $dataset.$aTableName;"
  val badSimpleSelect = s"SELECT * ForM $dataset.$aTableName"
  val simpleSelectNoSemicolon = s"""SELECT * FROM $dataset.$aTableName"""

  "#analyzeRawSelect" should {
    "return result" in {
      val result = getStreamResult(connector.analyzeRawSelect(simpleSelect))

      result shouldEqual Seq(
        Seq("totalBytesProcessed", "jobComplete", "cacheHit"),
        Seq("0", "true", "false")
      )
    }
  }
}

