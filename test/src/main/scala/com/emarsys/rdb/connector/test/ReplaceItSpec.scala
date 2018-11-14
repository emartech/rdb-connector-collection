package com.emarsys.rdb.connector.test

import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper.{BooleanValue, StringValue}
import com.emarsys.rdb.connector.common.models.DataManipulation.Record
import com.emarsys.rdb.connector.common.models.Errors.FailedValidation
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import com.emarsys.rdb.connector.common.models.ValidateDataManipulation.ValidationResult.NonExistingFields
import com.emarsys.rdb.connector.common.models.{Connector, SimpleSelect}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._

trait ReplaceItSpec extends WordSpecLike with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {
  val connector: Connector
  def initDb(): Unit
  def cleanUpDb(): Unit
  implicit val materializer: Materializer

  val uuid = uuidGenerate
  val tableName = s"replace_tables_table_$uuid"

  val awaitTimeout = 5.seconds
  val queryTimeout = 5.seconds

  override def beforeEach(): Unit = {
    initDb()
  }

  override def afterEach(): Unit = {
    cleanUpDb()
  }

  override def afterAll(): Unit = {
    connector.close()
  }

  val simpleSelect = SimpleSelect(AllField, TableName(tableName),
    where = Some(
      EqualToValue(FieldName("A1"), Value("vxxx"))
    ))
  val simpleSelectT = SimpleSelect(AllField, TableName(tableName),
    where = Some(
      EqualToValue(FieldName("A1"), Value("v1new"))
    ))
  val simpleSelectF = SimpleSelect(AllField, TableName(tableName),
    where = Some(
      EqualToValue(FieldName("A1"), Value("v2new"))
    ))
  val simpleSelectT2 = SimpleSelect(AllField, TableName(tableName),
    where = Some(
      EqualToValue(FieldName("A1"), Value("v3new"))
    ))

  val replaceMultipleData: Seq[Record] =  Seq(
    Map("A1" -> StringValue("v1new"), "A3" -> BooleanValue(true)),
    Map("A1" -> StringValue("v2new"), "A3" -> BooleanValue(false)),
    Map("A1" -> StringValue("v3new"), "A3" -> BooleanValue(false)))

  val replaceSingleData: Seq[Record] =  Seq(
    Map("A1" -> StringValue("vxxx"), "A3" -> BooleanValue(true)))

  val replaceNonExistingFieldFieldData: Seq[Record] =  Seq(
    Map("a" -> StringValue("1")),
    Map("a" -> StringValue("2")))

  s"ReplaceSpec $uuid" when {

    "#replace" should {

      "validation error" in {
        Await.result(connector.replaceData(tableName, replaceNonExistingFieldFieldData), awaitTimeout) shouldBe Left(FailedValidation(NonExistingFields(Set("a"))))
      }

      "replace successfully with zero record" in {
        Await.result(connector.replaceData(tableName, Seq.empty), awaitTimeout) shouldBe Right(0)
        Await.result(connector.simpleSelect(simpleSelectAllWithExpectedResultSize(0), queryTimeout), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(0)}

      "replace successfully with one record" in {
        Await.result(connector.replaceData(tableName, replaceSingleData), awaitTimeout) shouldBe Right(1)
        Await.result(connector.simpleSelect(simpleSelectAllWithExpectedResultSize(2), queryTimeout), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(2)
        Await.result(connector.simpleSelect(simpleSelect, queryTimeout), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(2)
      }

      "replace successfully with more records" in {
        Await.result(connector.replaceData(tableName, replaceMultipleData), awaitTimeout) shouldBe Right(3)
        Await.result(connector.simpleSelect(simpleSelectAllWithExpectedResultSize(4), queryTimeout), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(4)
        Await.result(connector.simpleSelect(simpleSelectT, queryTimeout), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(2)
        Await.result(connector.simpleSelect(simpleSelectF, queryTimeout), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(2)
        Await.result(connector.simpleSelect(simpleSelectT2, queryTimeout), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(2)

      }
    }
  }

  private def simpleSelectAllWithExpectedResultSize(number: Int) = SimpleSelect(AllField, TableName(tableName),
    where = Some(
      Or(Seq(EqualToValue(FieldName("A1"), Value(number.toString)), NotNull(FieldName("A1"))))))


}
