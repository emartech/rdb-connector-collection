package com.emarsys.rdb.connector.test

import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.emarsys.rdb.connector.common.models.{Connector, SimpleSelect}
import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper.{BooleanValue, IntValue, NullValue, StringValue}
import com.emarsys.rdb.connector.common.models.DataManipulation.UpdateDefinition
import com.emarsys.rdb.connector.common.models.Errors.FailedValidation
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import com.emarsys.rdb.connector.common.models.ValidateDataManipulation.ValidationResult.NonExistingFields
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, WordSpecLike}

import concurrent.duration._
import scala.concurrent.Await

/*
For positive results use the A table definition and preloaded data defined in the SimpleSelect.
Make sure you have index on A3.
*/

trait UpdateItSpec extends WordSpecLike with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {
  val connector: Connector
  def initDb(): Unit
  def cleanUpDb(): Unit
  implicit val materializer: Materializer

  val uuid = uuidGenerate
  val tableName = s"update_tables_table_$uuid"

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

  s"UpdateSpec $uuid" when {

    "#update" should {

      "validation error" in {
        val updateData = Seq(UpdateDefinition(Map("a" -> StringValue("1")), Map("a" -> StringValue("2"))))
        Await.result(connector.update(tableName, updateData), awaitTimeout) shouldBe Left(FailedValidation(NonExistingFields(Set("a"))))
      }

      "update successfully one definition" in {
        val updateData =  Seq(UpdateDefinition(Map("A3" -> BooleanValue(true)), Map("A2" -> IntValue(800))))
        val simpleSelect = SimpleSelect(AllField, TableName(tableName),
          where = Some(
            EqualToValue(FieldName("A2"), Value("800"))
          ))

        Await.result(connector.update(tableName, updateData), awaitTimeout) shouldBe Right(2)
        Await.result(connector.simpleSelect(simpleSelect, queryTimeout), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(2+1)
      }

      "update successfully more definition" in {
        val simpleSelectT = SimpleSelect(AllField, TableName(tableName),
          where = Some(
            EqualToValue(FieldName("A2"), Value("801"))
          ))
        val simpleSelectF = SimpleSelect(AllField, TableName(tableName),
          where = Some(
            EqualToValue(FieldName("A2"), Value("802"))
          ))
        val simpleSelectN = SimpleSelect(AllField, TableName(tableName),
          where = Some(
            EqualToValue(FieldName("A2"), Value("803"))
          ))


        val updateData =  Seq(
          UpdateDefinition(Map("A3" -> BooleanValue(true)), Map("A2" -> IntValue(801))),
          UpdateDefinition(Map("A3" -> BooleanValue(false)), Map("A2" -> IntValue(802))),
          UpdateDefinition(Map("A3" -> NullValue), Map("A2" -> IntValue(803)))
        )

        Await.result(connector.update(tableName, updateData), awaitTimeout) shouldBe Right(7)
        Await.result(connector.simpleSelect(simpleSelectT, queryTimeout), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(2+1)
        Await.result(connector.simpleSelect(simpleSelectF, queryTimeout), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(3+1)
        Await.result(connector.simpleSelect(simpleSelectN, queryTimeout), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(2+1)
      }
    }
  }

}
