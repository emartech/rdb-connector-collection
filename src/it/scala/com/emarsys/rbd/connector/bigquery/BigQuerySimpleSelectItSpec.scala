package com.emarsys.rbd.connector.bigquery

import java.util.UUID

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import akka.util.Timeout
import com.emarsys.rbd.connector.bigquery.utils.SelectDbInitHelper
import com.emarsys.rdb.connector.common.models.SimpleSelect
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import org.scalatest._

import scala.concurrent.Await
import scala.concurrent.duration._

class BigQuerySimpleSelectItSpec extends TestKit(ActorSystem()) /*with SimpleSelectItSpec*/ with WordSpecLike with Matchers with BeforeAndAfterAll with SelectDbInitHelper {

  override implicit val sys: ActorSystem = system
  override implicit val materializer: ActorMaterializer = ActorMaterializer()
  override implicit val timeout: Timeout = 5.seconds

  val uuid: String = UUID.randomUUID().toString.replace("-", "")

  val postfixTableName = s"_simple_select_table_$uuid"

  val aTableName = s"a$postfixTableName"
  val bTableName = s"b$postfixTableName"


  override def afterAll(): Unit = {
    cleanUpDb()
    connector.close()
    system.terminate()
  }

  override def beforeAll(): Unit = {
    initDb()
  }

  def checkResultWithoutRowOrder(result: Seq[Seq[String]], expected: Seq[Seq[String]]): Unit = {
    result.size shouldEqual expected.size
    result.head.map(_.toUpperCase) shouldEqual expected.head.map(_.toUpperCase)
    result.foreach(expected contains _)
  }

  def getSimpleSelectResult(simpleSelect: SimpleSelect): Seq[Seq[String]] = {
    val resultE = Await.result(connector.simpleSelect(simpleSelect), timeout.duration)

    resultE shouldBe a[Right[_, _]]
    val resultStream: Source[Seq[String], NotUsed] = resultE.right.get

    Await.result(resultStream.runWith(Sink.seq), timeout.duration * 20)
  }

  s"SimpleSelectItSpec $uuid" when {

    "#simpleSelect FIELDS" should {
      "list table values" in {
        val simpleSelect = SimpleSelect(AllField, TableName(aTableName))

        val result = getSimpleSelectResult(simpleSelect)

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

      "list table with specific values" in {
        val simpleSelect = SimpleSelect(AllField, TableName(bTableName))

        val result = getSimpleSelectResult(simpleSelect)

        checkResultWithoutRowOrder(result, Seq(
          Seq("B1", "B2", "B3", "B4"),
          Seq("b,1", "b.1", "b:1", "b\"1"),
          Seq("b;2", "b\\2", "b'2", "b=2"),
          Seq("b!3", "b@3", "b#3", null),
          Seq("b$4", "b%4", "b 4", null)
        ))
      }

      "list table values with specific fields" in {
        val simpleSelect = SimpleSelect(SpecificFields(Seq(FieldName("A3"), FieldName("A1"))), TableName(aTableName))

        val result = getSimpleSelectResult(simpleSelect)

        checkResultWithoutRowOrder(result, Seq(
          Seq("A3", "A1"),
          Seq("1", "v1"),
          Seq("0", "v2"),
          Seq("1", "v3"),
          Seq("0", "v4"),
          Seq("0", "v5"),
          Seq(null, "v6"),
          Seq(null, "v7")
        ))
      }

    }
    "#simpleSelect LIMIT" should {

      "list table values with LIMIT 2" in {
        val simpleSelect = SimpleSelect(AllField, TableName(aTableName), limit = Some(2))

        val result = getSimpleSelectResult(simpleSelect)

        result.size shouldEqual 3
        result.head.map(_.toUpperCase) shouldEqual Seq("A1", "A2", "A3").map(_.toUpperCase)
      }
    }

    "#simpleSelect simple WHERE" should {
      "list table values with IS NULL" in {
        val simpleSelect = SimpleSelect(AllField, TableName(aTableName), where = Some(IsNull(FieldName("A2"))))

        val result = getSimpleSelectResult(simpleSelect)

        checkResultWithoutRowOrder(result, Seq(
          Seq("A1", "A2", "A3"),
          Seq("v5", null, "0"),
          Seq("v7", null, null)
        ))
      }

      "list table values with NOT NULL" in {
        val simpleSelect = SimpleSelect(AllField, TableName(aTableName), where = Some(NotNull(FieldName("A2"))))

        val result = getSimpleSelectResult(simpleSelect)

        checkResultWithoutRowOrder(result, Seq(
          Seq("A1", "A2", "A3"),
          Seq("v1", "1", "1"),
          Seq("v2", "2", "0"),
          Seq("v3", "3", "1"),
          Seq("v4", "-4", "0"),
          Seq("v6", "6", null)
        ))
      }

      "list table values with EQUAL" in {
        val simpleSelect = SimpleSelect(AllField, TableName(aTableName), where = Some(EqualToValue(FieldName("A1"), Value("v3"))))

        val result = getSimpleSelectResult(simpleSelect)

        checkResultWithoutRowOrder(result, Seq(
          Seq("A1", "A2", "A3"),
          Seq("v3", "3", "1")
        ))
      }
    }

    "#simpleSelect compose WHERE" should {
      "list table values with OR" in {
        val simpleSelect = SimpleSelect(AllField, TableName(aTableName),
          where = Some(Or(Seq(
            EqualToValue(FieldName("A1"), Value("v1")),
            EqualToValue(FieldName("A1"), Value("v2")),
            IsNull(FieldName("A2"))
          ))))

        val result = getSimpleSelectResult(simpleSelect)

        checkResultWithoutRowOrder(result, Seq(
          Seq("A1", "A2", "A3"),
          Seq("v1", "1", "1"),
          Seq("v2", "2", "0"),
          Seq("v5", null, "0"),
          Seq("v7", null, null)
        ))
      }

      "list table values with AND" in {
        val simpleSelect = SimpleSelect(AllField, TableName(aTableName),
          where = Some(And(Seq(
            EqualToValue(FieldName("A1"), Value("v7")),
            IsNull(FieldName("A2"))
          ))))

        val result = getSimpleSelectResult(simpleSelect)

        checkResultWithoutRowOrder(result, Seq(
          Seq("A1", "A2", "A3"),
          Seq("v7", null, null)
        ))
      }

      "empty result when list table values with AND" in {
        val simpleSelect = SimpleSelect(AllField, TableName(aTableName),
          where = Some(And(Seq(
            EqualToValue(FieldName("A1"), Value("v7")),
            NotNull(FieldName("A2"))
          ))))

        val result = getSimpleSelectResult(simpleSelect)

        result shouldEqual Seq.empty
      }

      "list table values with OR + AND" in {
        val simpleSelect = SimpleSelect(AllField, TableName(aTableName),
          where = Some(Or(Seq(
            EqualToValue(FieldName("A1"), Value("v1")),
            And(Seq(
              IsNull(FieldName("A2")),
              IsNull(FieldName("A3"))
            ))
          ))))

        val result = getSimpleSelectResult(simpleSelect)

        checkResultWithoutRowOrder(result, Seq(
          Seq("A1", "A2", "A3"),
          Seq("v1", "1", "1"),
          Seq("v7", null, null)
        ))
      }

    }

  }
}
