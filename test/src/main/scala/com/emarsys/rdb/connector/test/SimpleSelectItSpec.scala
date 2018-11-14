package com.emarsys.rdb.connector.test

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import com.emarsys.rdb.connector.common.models.{Connector, SimpleSelect}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._

/*
For positive test results you need to implement an initDb function which creates two tables with the given names and
columns and must insert the sample data.

Tables:
A(A1: string, A2: ?int, A3: ?boolean)
B(B1: string, B2: string, B3: string, B4: ?string)
C(C1: string)

(We will reuse these table definitions with these data.
Please use unique and not null constraint on A1
If you want to reuse them too and your DB has indexes add index to (A2,A3) and A3 pls.)

Sample data:
A:
  ("v1", 1, true)
  ("v2", 2, false)
  ("v3", 3, true)
  ("v4", -4, false)
  ("v5", NULL, false)
  ("v6", 6, NULL)
  ("v7", NULL, NULL)

B:
  ("b,1", "b.1", "b:1", "b\"1")
  ("b;2", "b\\2", "b'2", "b=2")
  ("b!3", "b@3", "b#3", NULL)
  ("b$4", "b%4", "b 4", NULL)

C:
  ("c12")
  ("c12")
  ("c3")

 */
trait SimpleSelectItSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {
  val uuid = uuidGenerate

  val postfixTableName = s"_simple_select_table_$uuid"

  val aTableName = s"a$postfixTableName"
  val bTableName = s"b$postfixTableName"
  val cTableName = s"c$postfixTableName"
  val connector: Connector

  val awaitTimeout = 5.seconds
  val queryTimeout = 5.seconds

  implicit val materializer: Materializer

  override def beforeAll(): Unit = {
    initDb()
  }

  override def afterAll(): Unit = {
    cleanUpDb()
    connector.close()
  }

  def initDb(): Unit

  def cleanUpDb(): Unit

  def getSimpleSelectResult(simpleSelect: SimpleSelect): Seq[Seq[String]] = {
    getConnectorResult(connector.simpleSelect(simpleSelect, queryTimeout), awaitTimeout)
  }

  private val headerLineSize = 1

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
        val simpleSelect = SimpleSelect(SpecificFields(Seq(FieldName("A1"), FieldName("A3"))), TableName(aTableName))

        val result = getSimpleSelectResult(simpleSelect)

        checkResultWithoutRowOrder(result, Seq(
          Seq("A1", "A3"),
          Seq("v1", "1"),
          Seq("v2", "0"),
          Seq("v3", "1"),
          Seq("v4", "0"),
          Seq("v5", "0"),
          Seq("v6", null),
          Seq("v7", null)
        ))
      }
    }

    "#simpleSelect LIMIT" should {

      "list table values with LIMIT 2" in {
        val simpleSelect = SimpleSelect(AllField, TableName(aTableName), limit = Some(2))

        val result = getSimpleSelectResult(simpleSelect)

        result.size shouldEqual headerLineSize + 2
        result.head.map(_.toUpperCase) shouldEqual Seq("A1", "A2", "A3").map(_.toUpperCase)
      }
    }

    "#simpleSelect DISTINCT" should {

      "list table values without DISTINCT" in {
        val simpleSelect = SimpleSelect(AllField, TableName(cTableName))

        val result = getSimpleSelectResult(simpleSelect)

        result.size shouldEqual headerLineSize + 3
      }

      "list table values with DISTINCT" in {
        val simpleSelect = SimpleSelect(AllField, TableName(cTableName), distinct = Some(true))

        val result = getSimpleSelectResult(simpleSelect)

        result.size shouldEqual headerLineSize + 2
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

      "list table values with EQUAL on strings" in {
        val simpleSelect = SimpleSelect(AllField, TableName(aTableName), where = Some(EqualToValue(FieldName("A1"), Value("v3"))))

        val result = getSimpleSelectResult(simpleSelect)

        checkResultWithoutRowOrder(result, Seq(
          Seq("A1", "A2", "A3"),
          Seq("v3", "3", "1")
        ))
      }

      "list table values with EQUAL on numbers" in {
        val simpleSelect = SimpleSelect(AllField, TableName(aTableName), where = Some(EqualToValue(FieldName("A2"), Value("3"))))

        val result = getSimpleSelectResult(simpleSelect)

        checkResultWithoutRowOrder(result, Seq(
          Seq("A1", "A2", "A3"),
          Seq("v3", "3", "1")
        ))
      }

      "list table values with EQUAL on booleans" in {
        val simpleSelect = SimpleSelect(AllField, TableName(aTableName), where = Some(EqualToValue(FieldName("A3"), Value("1"))))

        val result = getSimpleSelectResult(simpleSelect)

        checkResultWithoutRowOrder(result, Seq(
          Seq("A1", "A2", "A3"),
          Seq("v1", "1", "1"),
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

