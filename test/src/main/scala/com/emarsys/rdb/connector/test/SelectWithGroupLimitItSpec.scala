package com.emarsys.rdb.connector.test

import akka.stream.Materializer
import com.emarsys.rdb.connector.common.models.Errors.SimpleSelectIsNotGroupableFormat
import com.emarsys.rdb.connector.common.models.{Connector, SimpleSelect}
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import concurrent.duration._
import scala.concurrent.Await
/*
For positive test results you need to implement an initDb function which creates a table with the given name and
columns and must insert the sample data.

Tables:
D(ID: int, NAME: string, DATA: string)

(We will reuse these table definitions with these data.

Sample data:
D:
  (1, "test1", "data1")
  (1, "test1", "data2")
  (1, "test1", "data3")
  (1, "test1", "data4")
  (2, "test2", "data5")
  (2, "test2", "data6")
  (2, "test2", "data7")
  (2, "test3", "data8")
  (3, "test4", "data9")


 */
trait SelectWithGroupLimitItSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {
  val uuid = uuidGenerate

  val postfixTableName = s"_select_w_grouplimit_table_$uuid"

  val tableName = s"d$postfixTableName"

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

  s"SelectWithGroupLimitItSpec $uuid" when {

    "gets a non OR query and limit it" in {
      val simpleSelect = SimpleSelect(AllField, TableName(tableName),
        where = Some(And(Seq(
          EqualToValue(FieldName("ID"), Value("1")),
          EqualToValue(FieldName("NAME"), Value("test1"))
        ))))

      val result = getConnectorResult(connector.selectWithGroupLimit(simpleSelect, 2, queryTimeout), awaitTimeout)
      result.size shouldBe 3
      result.head.map(_.toUpperCase) shouldBe Seq("ID", "NAME", "DATA")
      result.tail.foreach(_(0) shouldBe  "1")
      result.tail.foreach(_(1) shouldBe  "test1")
    }

    "gets a good OR query and limit it" in {
      val simpleSelect = SimpleSelect(AllField, TableName(tableName),
        where = Some(Or(Seq(
          And(Seq(
            EqualToValue(FieldName("ID"), Value("1")),
            EqualToValue(FieldName("NAME"), Value("test1"))
          )),
          And(Seq(
            EqualToValue(FieldName("ID"), Value("2")),
            EqualToValue(FieldName("NAME"), Value("test2"))
          )),
          And(Seq(
            EqualToValue(FieldName("ID"), Value("2")),
            EqualToValue(FieldName("NAME"), Value("test3"))
          )),
          And(Seq(
            EqualToValue(FieldName("ID"), Value("7")),
            EqualToValue(FieldName("NAME"), Value("test123"))
          ))
        ))))

      val result = getConnectorResult(connector.selectWithGroupLimit(simpleSelect, 2, queryTimeout), awaitTimeout)
      result.size shouldBe 6
      result.head.map(_.toUpperCase) shouldBe Seq("ID", "NAME", "DATA")
      val grouped = result.tail.groupBy(s => (s(0), s(1)))
      grouped.size shouldBe 3
      grouped.keys.toSet shouldBe Set(("1", "test1"), ("2", "test2"), ("2", "test3"))
    }

    "gets a bad OR query and fails" in {
      val simpleSelect = SimpleSelect(AllField, TableName(tableName),
        where = Some(Or(Seq(
          And(Seq(
            EqualToValue(FieldName("ID"), Value("1")),
            EqualToValue(FieldName("NAME"), Value("test1"))
          )),
          And(Seq(
            EqualToValue(FieldName("ID"), Value("2"))
          ))
        ))))
      val result = Await.result(connector.selectWithGroupLimit(simpleSelect, 2, queryTimeout), awaitTimeout)
      result shouldBe a [Left[_,_]]
      result.left.get shouldBe SimpleSelectIsNotGroupableFormat(simpleSelect.toString)
    }

    "gets a good OR query but no results" in {
      val simpleSelect = SimpleSelect(AllField, TableName(tableName),
        where = Some(Or(Seq(
          And(Seq(
            EqualToValue(FieldName("ID"), Value("7"))
          )),
          And(Seq(
            EqualToValue(FieldName("ID"), Value("8"))
          ))
        ))))

      val result = getConnectorResult(connector.selectWithGroupLimit(simpleSelect, 2, queryTimeout), awaitTimeout)
      result.size shouldBe 0
    }
  }
}
