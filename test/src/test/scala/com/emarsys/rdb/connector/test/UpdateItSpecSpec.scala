package com.emarsys.rdb.connector.test

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper.{BooleanValue, IntValue, NullValue, StringValue}
import com.emarsys.rdb.connector.common.models.DataManipulation.UpdateDefinition
import com.emarsys.rdb.connector.common.models.Errors.FailedValidation
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import com.emarsys.rdb.connector.common.models.ValidateDataManipulation.ValidationResult.NonExistingFields
import com.emarsys.rdb.connector.common.models.{Connector, SimpleSelect}
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfterAll
import org.scalatest.mockito.MockitoSugar

import scala.concurrent.Future

class UpdateItSpecSpec extends TestKit(ActorSystem()) with UpdateItSpec with MockitoSugar with BeforeAndAfterAll {

    implicit val materializer: Materializer = ActorMaterializer()

    implicit val executionContext = system.dispatcher

    override val connector = mock[Connector]

    override def initDb(): Unit = ()

    override def cleanUpDb(): Unit = ()

    override def afterAll {
      TestKit.shutdownActorSystem(system)
    }

  val updateData =  Seq(
    UpdateDefinition(Map("A3" -> BooleanValue(true)), Map("A2" -> IntValue(801))),
    UpdateDefinition(Map("A3" -> BooleanValue(false)), Map("A2" -> IntValue(802))),
    UpdateDefinition(Map("A3" -> NullValue), Map("A2" -> IntValue(803)))
  )

  when(connector.update(tableName, Seq(UpdateDefinition(Map("A3" -> BooleanValue(true)), Map("A2" -> IntValue(800)))))).thenReturn(Future.successful(Right(2)))
  when(connector.update(tableName, updateData)).thenReturn(Future.successful(Right(7)))
  when(connector.update(tableName, Seq(UpdateDefinition(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))))).thenReturn(Future.successful(Left(FailedValidation(NonExistingFields(Set("a"))))))


  when(connector.simpleSelect(SimpleSelect(AllField, TableName(tableName),
    where = Some(
      EqualToValue(FieldName("A2"), Value("800"))
    )), queryTimeout)).thenReturn(Future(Right(Source(List(Seq("A2"), Seq("a"),Seq("s"))))))

  when(connector.simpleSelect(SimpleSelect(AllField, TableName(tableName),
    where = Some(
      EqualToValue(FieldName("A2"), Value("801"))
    )), queryTimeout)).thenReturn(Future(Right(Source(List(Seq("A2"), Seq("a"),Seq("s"))))))

  when(connector.simpleSelect(SimpleSelect(AllField, TableName(tableName),
    where = Some(
      EqualToValue(FieldName("A2"), Value("802"))
    )), queryTimeout)).thenReturn(Future(Right(Source(List(Seq("A2"), Seq("a"),Seq("s"), Seq("d"))))))

  when(connector.simpleSelect(SimpleSelect(AllField, TableName(tableName),
    where = Some(
      EqualToValue(FieldName("A2"), Value("803"))
    )), queryTimeout)).thenReturn(Future(Right(Source(List(Seq("A2"), Seq("a"),Seq("s"))))))

}
