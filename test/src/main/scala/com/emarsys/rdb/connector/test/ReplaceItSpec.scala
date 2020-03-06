package com.emarsys.rdb.connector.test

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.{Connector, SimpleSelect}
import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper.{BooleanValue, StringValue}
import com.emarsys.rdb.connector.common.models.DataManipulation.Record
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorName, Fields}
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

trait ReplaceItSpec extends AnyWordSpecLike with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {
  val connector: Connector
  def initDb(): Unit
  def cleanUpDb(): Unit
  implicit val materializer: Materializer

  val uuid      = uuidGenerate
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

  val simpleSelect = SimpleSelect(
    AllField,
    TableName(tableName),
    where = Some(
      EqualToValue(FieldName("A1"), Value("vxxx"))
    )
  )
  val simpleSelectT = SimpleSelect(
    AllField,
    TableName(tableName),
    where = Some(
      EqualToValue(FieldName("A1"), Value("v1new"))
    )
  )
  val simpleSelectF = SimpleSelect(
    AllField,
    TableName(tableName),
    where = Some(
      EqualToValue(FieldName("A1"), Value("v2new"))
    )
  )
  val simpleSelectT2 = SimpleSelect(
    AllField,
    TableName(tableName),
    where = Some(
      EqualToValue(FieldName("A1"), Value("v3new"))
    )
  )

  val replaceMultipleData: Seq[Record] = Seq(
    Map("A1" -> StringValue("v1new"), "A3" -> BooleanValue(true)),
    Map("A1" -> StringValue("v2new"), "A3" -> BooleanValue(false)),
    Map("A1" -> StringValue("v3new"), "A3" -> BooleanValue(false))
  )

  val replaceSingleData: Seq[Record] = Seq(Map("A1" -> StringValue("vxxx"), "A3" -> BooleanValue(true)))

  val replaceNonExistingFieldFieldData: Seq[Record] = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))

  private def await[T](f: Future[T]): T = {
    Await.result(f, awaitTimeout)
  }

  private def awaitStream(f: ConnectorResponse[Source[Seq[String], NotUsed]]): Either[DatabaseError, Int] = {
    Await.result(f, awaitTimeout).map(stream => await(stream.runWith(Sink.seq)).size)
  }

  s"ReplaceSpec $uuid" when {

    "#replace" should {

      "validation error" in {
        await(connector.replaceData(tableName, replaceNonExistingFieldFieldData)) shouldBe Left(
          DatabaseError.validation(ErrorName.MissingFields, Some(Fields(List("a"))))
        )
      }

      "replace successfully with zero record" in {
        await(connector.replaceData(tableName, Seq.empty)) shouldBe Right(0)
        awaitStream(connector.simpleSelect(simpleSelectAllWithExpectedResultSize(0), queryTimeout)) shouldBe Right(0)
      }

      "replace successfully with one record" in {
        await(connector.replaceData(tableName, replaceSingleData)) shouldBe Right(1)
        awaitStream(connector.simpleSelect(simpleSelectAllWithExpectedResultSize(2), queryTimeout)) shouldBe Right(2)
        awaitStream(connector.simpleSelect(simpleSelect, queryTimeout)) shouldBe Right(2)
      }

      "replace successfully with more records" in {
        await(connector.replaceData(tableName, replaceMultipleData)) shouldBe Right(3)
        awaitStream(connector.simpleSelect(simpleSelectAllWithExpectedResultSize(4), queryTimeout)) shouldBe Right(4)
        awaitStream(connector.simpleSelect(simpleSelectT, queryTimeout)) shouldBe Right(2)
        awaitStream(connector.simpleSelect(simpleSelectF, queryTimeout)) shouldBe Right(2)
        awaitStream(connector.simpleSelect(simpleSelectT2, queryTimeout)) shouldBe Right(2)
      }
    }
  }

  private def simpleSelectAllWithExpectedResultSize(number: Int) =
    SimpleSelect(
      AllField,
      TableName(tableName),
      where = Some(Or(Seq(EqualToValue(FieldName("A1"), Value(number.toString)), NotNull(FieldName("A1")))))
    )

}
