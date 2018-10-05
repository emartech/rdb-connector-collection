package com.emarsys.rbd.connector.bigquery

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.testkit.TestKit
import akka.util.Timeout
import com.emarsys.rbd.connector.bigquery.utils.{SelectDbInitHelper, TestHelper}
import com.emarsys.rdb.connector.test.RawSelectItSpec
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.util.{Failure, Try}

class BigQueryRawSelectItSpec extends TestKit(ActorSystem()) with RawSelectItSpec with SelectDbInitHelper with WordSpecLike with Matchers with BeforeAndAfterAll {

  implicit override val sys: ActorSystem = system
  implicit override val materializer: ActorMaterializer = ActorMaterializer()
  implicit override val timeout: Timeout = Timeout(30.second)
  implicit override val queryTimeout: FiniteDuration = timeout.duration
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

    "cancel job" ignore {
      val slowSelect = s"""SELECT A.A1 FROM $dataset.$aTableName A
                         |JOIN $dataset.$aTableName B ON NOT A.A1 = B.A1
                         |JOIN $dataset.$aTableName C ON NOT A.A1 = C.A1
                         |JOIN $dataset.$aTableName D ON NOT A.A1 = D.A1
                         |JOIN $dataset.$aTableName E ON NOT A.A1 = E.A1
                         |JOIN $dataset.$aTableName F ON NOT A.A1 = F.A1
                         |JOIN $dataset.$aTableName G ON NOT A.A1 = G.A1
                         |JOIN $dataset.$aTableName H ON NOT A.A1 = H.A1
                         |JOIN $dataset.$aTableName I ON NOT A.A1 = I.A1
                         |;""".stripMargin

      val source = Await.result(connector.rawSelect(slowSelect, None, 13.second), 30.seconds).right.get

      val result = Try(Await.result(source.runWith(Sink.seq),70.seconds).size)

      result should matchPattern { case k: Failure[_] => }
    }

  }
}

