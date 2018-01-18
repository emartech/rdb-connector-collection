package com.emarsys.rbd.connector.bigquery

import java.util.UUID

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import akka.util.Timeout
import com.emarsys.rbd.connector.bigquery.utils.{SelectDbInitHelper, TestHelper}
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.test.RawSelectItSpec
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._

class BigQueryRawSelectItSpec extends TestKit(ActorSystem()) /*with RawSelectItSpec*/ with SelectDbInitHelper with WordSpecLike with Matchers with BeforeAndAfterAll{

  implicit override val sys: ActorSystem = system
  implicit override val materializer: ActorMaterializer = ActorMaterializer()
  implicit override val timeout: Timeout = Timeout(3.second)

  val uuid = UUID.randomUUID().toString.replace("-", "")

  val postfixTableName = s"_raw_select_table_$uuid"

  val aTableName = s"a$postfixTableName"
  val bTableName = s"b$postfixTableName"

  val awaitTimeout = 5.seconds

  override def afterAll(): Unit = {
    cleanUpDb()
    TestKit.shutdownActorSystem(sys)
    connector.close()
  }

  override def beforeAll(): Unit = {
    initDb()
  }

  val dataset = TestHelper.TEST_CONNECTION_CONFIG.dataset

  val simpleSelect = s"SELECT * FROM $dataset.$aTableName;"
  val badSimpleSelect = s"SELECT * ForM $dataset.$aTableName"
  val simpleSelectNoSemicolon = s"""SELECT * FROM $dataset.$aTableName"""


  "#rawSelect" should {
    "list table values" in {

      val result = getStreamResult(connector.rawSelect(simpleSelect, None))

      checkResultWithoutRowOrder(result, Seq(
        Seq("A1", "A2", "A3"),
        Seq("v1", "1", "1"),
        Seq("v2", "2", "0"),
        Seq("v3", "3", "1"),
        Seq("v4", "-4", "0"),
        Seq("v5", null, "0"),
        Seq("v6", "6", null),
        Seq("v7", null, null)
      ))
    }

    "list table values with limit" in {

      val limit = 2

      val result = getStreamResult(connector.rawSelect(simpleSelect, Option(limit)))

      result.size shouldEqual limit + 1
    }

    "fail the future if query is bad" in {

      assertThrows[Exception](Await.result(connector.rawSelect(badSimpleSelect, None).flatMap(_.right.get.runWith(Sink.seq)), awaitTimeout))
    }
  }

  "#projectedRawSelect" should {

    "project one col as expected" in {

      val result = getStreamResult(connector.projectedRawSelect(simpleSelect, Seq("A1")))

      checkResultWithoutRowOrder(result, Seq(
        Seq("A1"),
        Seq("v1"),
        Seq("v2"),
        Seq("v3"),
        Seq("v4"),
        Seq("v5"),
        Seq("v6"),
        Seq("v7")
      ))

    }

    "project more col as expected" in {
      val result = getStreamResult(connector.projectedRawSelect(simpleSelect, Seq("A2", "A3")))

      checkResultWithoutRowOrder(result, Seq(
        Seq("A2", "A3"),
        Seq("1", "1"),
        Seq("2", "0"),
        Seq("3", "1"),
        Seq("-4", "0"),
        Seq(null, "0"),
        Seq("6", null),
        Seq(null, null)
      ))
    }
  }

  def checkResultWithoutRowOrder(result: Seq[Seq[String]], expected: Seq[Seq[String]]): Unit = {
    result.size shouldEqual expected.size
    result.head.map(_.toUpperCase) shouldEqual expected.head.map(_.toUpperCase)
    result.foreach(expected contains _)
  }

  def getStreamResult(s: ConnectorResponse[Source[Seq[String], NotUsed]]): Seq[Seq[String]] = {
    val resultE = Await.result(s, awaitTimeout)

    resultE shouldBe a[Right[_, _]]
    val resultStream: Source[Seq[String], NotUsed] = resultE.right.get

    Await.result(resultStream.runWith(Sink.seq), awaitTimeout)
  }
}

