package com.emarsys.rdb.connector.mssql

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl.Sink
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper.{BooleanValue, StringValue}
import com.emarsys.rdb.connector.common.models.DataManipulation.Record
import com.emarsys.rdb.connector.common.models.SimpleSelect
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import com.emarsys.rdb.connector.mssql.utils.SelectDbInitHelper
import com.emarsys.rdb.connector.test.InsertItSpec

import scala.concurrent.Await

class MsSqlInsertSpec extends TestKit(ActorSystem("MsSqlInsertSpec")) with InsertItSpec with SelectDbInitHelper {

  val aTableName: String = tableName
  val bTableName: String = s"temp_$uuid"

  implicit override val materializer: Materializer = ActorMaterializer()

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  val simpleSelectExisting = SimpleSelect(
    AllField,
    TableName(tableName),
    where = Some(
      EqualToValue(FieldName("A1"), Value("v1"))
    )
  )

  val newA1                      = "newkey"
  val insertNewData: Seq[Record] = Seq(Map("A1" -> StringValue(newA1), "A3" -> BooleanValue(true)))
  val simpleSelectNewlyInserted = SimpleSelect(
    AllField,
    TableName(tableName),
    where = Some(
      EqualToValue(FieldName("A1"), Value(newA1))
    )
  )

  s"InsertIgnoreSpec $uuid" when {

    "#insertIgnore" should {
      "ignore if inserting existing record" in {
        Await.result(connector.insertIgnore(tableName, insertExistingData), awaitTimeout) shouldBe Right(0)

        Await
          .result(connector.simpleSelect(simpleSelectExisting, queryTimeout), awaitTimeout)
          .map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(2)
      }

      "ignore only the duplicates" in {
        val dataToInsert = insertExistingData ++ insertNewData
        Await.result(connector.insertIgnore(tableName, dataToInsert), awaitTimeout) shouldBe Right(1)

        Await
          .result(connector.simpleSelect(simpleSelectNewlyInserted, queryTimeout), awaitTimeout)
          .map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(2)
      }
    }
  }
}
