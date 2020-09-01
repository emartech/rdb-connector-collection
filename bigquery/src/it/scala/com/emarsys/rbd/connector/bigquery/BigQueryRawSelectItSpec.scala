package com.emarsys.rbd.connector.bigquery

import akka.actor.ActorSystem
import akka.testkit.TestKit
import akka.util.Timeout
import com.emarsys.rbd.connector.bigquery.utils.{SelectDbInitHelper, TestHelper}
import com.emarsys.rdb.connector.test._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

class BigQueryRawSelectItSpec
    extends TestKit(ActorSystem("BigQueryRawSelectItSpec"))
    with RawSelectItSpec
    with SelectDbInitHelper
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  implicit override val sys: ActorSystem                           = system
  implicit override val timeout: Timeout                           = Timeout(30.second)
  implicit override val queryTimeout: FiniteDuration               = timeout.duration
  implicit override val executionContext: ExecutionContextExecutor = sys.dispatcher

  override val awaitTimeout = 30.seconds

  override def afterAll(): Unit = {
    cleanUpDb()
    shutdown()
  }

  override def beforeAll(): Unit = {
    initDb()
  }

  val dataset = TestHelper.TEST_CONNECTION_CONFIG.dataset

  val simpleSelect            = s"SELECT * FROM $dataset.$aTableName;"
  val badSimpleSelect         = s"SELECT * ForM $dataset.$aTableName"
  val simpleSelectNoSemicolon = s"""SELECT * FROM $dataset.$aTableName"""

  "#analyzeRawSelect" should {
    "return result" in {
      val result = getConnectorResult(connector.analyzeRawSelect(simpleSelect), awaitTimeout)

      result shouldEqual Seq(
        Seq("totalBytesProcessed", "jobComplete", "cacheHit"),
        Seq("0", "true", "false")
      )
    }
  }
}
