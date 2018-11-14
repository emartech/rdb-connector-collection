package com.emarsys.rdb.connector.test

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.models.Connector
import com.emarsys.rdb.connector.common.models.Errors.FailedValidation
import com.emarsys.rdb.connector.common.models.ValidateDataManipulation.ValidationResult.NonExistingFields
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfterAll
import org.scalatest.mockito.MockitoSugar

import scala.concurrent.Future

class InsertIgnoreItSpecSpec extends TestKit(ActorSystem()) with InsertItSpec with MockitoSugar with BeforeAndAfterAll {

  import com.emarsys.rdb.connector.utils.TestHelper._

  implicit val materializer: Materializer = ActorMaterializer()

  implicit val executionContext = system.dispatcher

  override val connector = mock[Connector]

  override def initDb(): Unit = ()

  override def cleanUpDb(): Unit = ()

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }


  when(connector.insertIgnore(tableName, insertMultipleData)).thenReturn(Future.successful(Right(3)))
  when(connector.insertIgnore(tableName, insertFieldDataWithMissingFields)).thenReturn(Future.successful(Right(2)))
  when(connector.insertIgnore(tableName, insertSingleData)).thenReturn(Future.successful(Right(1)))
  when(connector.insertIgnore(tableName, insertNullData)).thenReturn(Future.successful(Right(1)))
  when(connector.insertIgnore(tableName, insertExistingData)).thenReturn(Future.successful(Right(0)))
  when(connector.insertIgnore(tableName, Seq.empty)).thenReturn(Future.successful(Right(0)))

  when(connector.insertIgnore(tableName, insertNonExistingFieldFieldData))
    .thenReturn(Future.successful(Left(FailedValidation(NonExistingFields(Set("a"))))))

  when(connector.simpleSelect(simpleSelectIsNull, queryTimeout))
    .thenReturn(Future(Right(Source(List(Seq("columnName"), Seq("vref1"),Seq("vref2"),Seq("v5"),Seq("v7"))))))


  Seq(
    simpleSelectF,
    simpleSelectT,
    simpleSelectT2,
    simpleSelectOneRecord,
    simpleSelectN
  ).foreach(selectUniqueValueMock(_, connector, queryTimeout))

  Seq(11, 10, 9, 8).foreach(selectExactNumberMock(_, tableName, connector, queryTimeout))

}
