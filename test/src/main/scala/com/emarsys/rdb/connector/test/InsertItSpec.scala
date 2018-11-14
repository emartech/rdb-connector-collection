package com.emarsys.rdb.connector.test

import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper.{BooleanValue, NullValue, StringValue}
import com.emarsys.rdb.connector.common.models.DataManipulation.Record
import com.emarsys.rdb.connector.common.models.Errors.FailedValidation
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import com.emarsys.rdb.connector.common.models.ValidateDataManipulation.ValidationResult.NonExistingFields
import com.emarsys.rdb.connector.common.models.{Connector, SimpleSelect}
import org.scalatest._

import scala.concurrent.Await
import scala.concurrent.duration._

trait InsertItSpec extends WordSpecLike with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {
  val connector: Connector
  def initDb(): Unit
  def cleanUpDb(): Unit
  implicit val materializer: Materializer

  val uuid = uuidGenerate
  val tableName = s"insert_tables_table_$uuid"

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

  val simpleSelectOneRecord = SimpleSelect(AllField, TableName(tableName),
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
  val simpleSelectN = SimpleSelect(AllField, TableName(tableName),
    where = Some(
      EqualToValue(FieldName("A1"), Value("vn"))
    ))

  val simpleSelectIsNull = SimpleSelect(AllField, TableName(tableName), where = Option(IsNull(FieldName("A3"))))


  val insertMultipleData: Seq[Record] =  Seq(
    Map("A1" -> StringValue("v1new"), "A3" -> BooleanValue(true)),
    Map("A1" -> StringValue("v2new"), "A3" -> BooleanValue(false)),
    Map("A1" -> StringValue("v3new"), "A3" -> BooleanValue(false)))

  val insertSingleData: Seq[Record] =  Seq(
    Map("A1" -> StringValue("vxxx"), "A3" -> BooleanValue(true)))

  val insertExistingData: Seq[Record] =  Seq(
    Map("A1" -> StringValue("v1"), "A3" -> BooleanValue(true)))

  val insertNullData: Seq[Record] =  Seq(
    Map("A1" -> StringValue("vn"), "A3" -> NullValue))

  val insertFieldDataWithMissingFields: Seq[Record] =  Seq(
    Map("A1" -> StringValue("vref1")),
    Map("A1" -> StringValue("vref2")))

  val insertNonExistingFieldFieldData: Seq[Record] =  Seq(
    Map("a" -> StringValue("1")),
    Map("a" -> StringValue("2")))



  s"InsertIgnoreSpec $uuid" when {

    "#insertIgnore" should {

      "validation error" in {
        Await.result(connector.insertIgnore(tableName, insertNonExistingFieldFieldData), awaitTimeout) shouldBe Left(FailedValidation(NonExistingFields(Set("a"))))
      }

      "insert successfully zero record" in {
        Await.result(connector.insertIgnore(tableName, Seq.empty), awaitTimeout) shouldBe Right(0)
        Await.result(connector.simpleSelect(simpleSelectAllWithExpectedResultSize(8), queryTimeout), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(8)
      }

      "insert successfully one record" in {
        Await.result(connector.insertIgnore(tableName, insertSingleData), awaitTimeout) shouldBe Right(1)
        Await.result(connector.simpleSelect(simpleSelectAllWithExpectedResultSize(9), queryTimeout), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(9)
        Await.result(connector.simpleSelect(simpleSelectOneRecord, queryTimeout), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(2)
      }

      "insert successfully more records" in {
        Await.result(connector.insertIgnore(tableName, insertMultipleData), awaitTimeout) shouldBe Right(3)
        Await.result(connector.simpleSelect(simpleSelectAllWithExpectedResultSize(11), queryTimeout), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(11)
        Await.result(connector.simpleSelect(simpleSelectT, queryTimeout), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(2)
        Await.result(connector.simpleSelect(simpleSelectF, queryTimeout), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(2)
        Await.result(connector.simpleSelect(simpleSelectT2, queryTimeout), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(2)

      }

      "successfully insert NULL values" in {
        Await.result(connector.insertIgnore(tableName, insertNullData), awaitTimeout) shouldBe Right(1)
        Await.result(connector.simpleSelect(simpleSelectAllWithExpectedResultSize(9), queryTimeout), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(9)
        Await.result(connector.simpleSelect(simpleSelectN, queryTimeout), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(2)
      }

      "successfully insert if all compulsory fields are defined, fill undefined values with NULL" in {
        Await.result(connector.insertIgnore(tableName, insertFieldDataWithMissingFields), awaitTimeout) shouldBe Right(2)
        Await.result(connector.simpleSelect(simpleSelectAllWithExpectedResultSize(10), queryTimeout), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(10)
        Await.result(connector.simpleSelect(simpleSelectIsNull, queryTimeout), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(5)

      }
    }
  }

  protected def simpleSelectAllWithExpectedResultSize(number: Int) = SimpleSelect(AllField, TableName(tableName),
    where = Some(
      Or(Seq(EqualToValue(FieldName("A1"), Value(number.toString)), NotNull(FieldName("A1"))))))

}
