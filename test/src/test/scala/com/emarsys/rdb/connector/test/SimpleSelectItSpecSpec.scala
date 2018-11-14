package com.emarsys.rdb.connector.test

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl.Source
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.models.{Connector, SimpleSelect}
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.mockito.MockitoSugar

import scala.concurrent.Future

class SimpleSelectItSpecSpec extends TestKit(ActorSystem()) with SimpleSelectItSpec with MockitoSugar with BeforeAndAfterAll {

  implicit val materializer: Materializer = ActorMaterializer()

  implicit val executionContext = system.dispatcher

  override val connector = mock[Connector]

  override def initDb(): Unit = ()

  override def cleanUpDb(): Unit = ()

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  when(connector.simpleSelect(SimpleSelect(AllField, TableName(aTableName)), queryTimeout)).thenReturn(Future(Right(Source(Seq(
    Seq("A1", "A2", "A3"),
    Seq("v1", "1", "1"),
    Seq("v3", "3", "1"),
    Seq("v2", "2", "0"),
    Seq("v6", "6", null),
    Seq("v4", "-4", "0"),
    Seq("v5", null, "0"),
    Seq("v7", null, null)
  ).to[scala.collection.immutable.Seq]))))

  when(connector.simpleSelect(SimpleSelect(AllField, TableName(bTableName)), queryTimeout)).thenReturn(Future(Right(Source(Seq(
    Seq("B1", "B2", "B3", "B4"),
    Seq("b!3", "b@3", "b#3", null),
    Seq("b;2", "b\\2", "b'2", "b=2"),
    Seq("b,1", "b.1", "b:1", "b\"1"),
    Seq("b$4", "b%4", "b 4", null)
  ).to[scala.collection.immutable.Seq]))))

  when(connector.simpleSelect(SimpleSelect(AllField, TableName(cTableName)), queryTimeout)).thenReturn(Future(Right(Source(Seq(
    Seq("C"),
    Seq("c12"),
    Seq("c12"),
    Seq("c3"),
  ).to[scala.collection.immutable.Seq]))))

  when(connector.simpleSelect(SimpleSelect(AllField, TableName(cTableName), distinct = Some(true)), queryTimeout)).thenReturn(Future(Right(Source(Seq(
    Seq("C"),
    Seq("c12"),
    Seq("c3"),
  ).to[scala.collection.immutable.Seq]))))

  when(connector.simpleSelect(SimpleSelect(SpecificFields(Seq(FieldName("A1"), FieldName("A3"))), TableName(aTableName)), queryTimeout)).thenReturn(Future(Right(Source(Seq(
    Seq("A1", "A3"),
    Seq("v1", "1"),
    Seq("v2", "0"),
    Seq("v6", null),
    Seq("v3", "1"),
    Seq("v4", "0"),
    Seq("v5", "0"),
    Seq("v7", null)
  ).to[scala.collection.immutable.Seq]))))

  when(connector.simpleSelect(SimpleSelect(AllField, TableName(aTableName), limit = Some(2)), queryTimeout)).thenReturn(Future(Right(Source(Seq(
    Seq("A1", "A2", "A3"),
    Seq("v3", "3", "1"),
    Seq("v4", "-4", "0")
  ).to[scala.collection.immutable.Seq]))))

  when(connector.simpleSelect(SimpleSelect(AllField, TableName(aTableName), where = Some(IsNull(FieldName("A2")))), queryTimeout)).thenReturn(Future(Right(Source(Seq(
    Seq("A1", "A2", "A3"),
    Seq("v5", null, "0"),
    Seq("v7", null, null)
  ).to[scala.collection.immutable.Seq]))))

  when(connector.simpleSelect(SimpleSelect(AllField, TableName(aTableName), where = Some(NotNull(FieldName("A2")))), queryTimeout)).thenReturn(Future(Right(Source(Seq(
    Seq("A1", "A2", "A3"),
    Seq("v2", "2", "0"),
    Seq("v4", "-4", "0"),
    Seq("v3", "3", "1"),
    Seq("v1", "1", "1"),
    Seq("v6", "6", null)
  ).to[scala.collection.immutable.Seq]))))

  when(connector.simpleSelect(SimpleSelect(AllField, TableName(aTableName),
    where = Some(EqualToValue(FieldName("A1"), Value("v3")))), queryTimeout)
  ).thenReturn(Future(Right(Source(Seq(
    Seq("A1", "A2", "A3"),
    Seq("v3", "3", "1")
  ).to[scala.collection.immutable.Seq]))))

  when(connector.simpleSelect(SimpleSelect(AllField, TableName(aTableName),
    where = Some(EqualToValue(FieldName("A2"), Value("3")))), queryTimeout
  )).thenReturn(Future(Right(Source(Seq(
    Seq("A1", "A2", "A3"),
    Seq("v3", "3", "1")
  ).to[scala.collection.immutable.Seq]))))

  when(connector.simpleSelect(SimpleSelect(AllField, TableName(aTableName),
    where = Some(EqualToValue(FieldName("A3"), Value("1")))), queryTimeout
  )).thenReturn(Future(Right(Source(Seq(
    Seq("A1", "A2", "A3"),
    Seq("v1", "1", "1"),
    Seq("v3", "3", "1")
  ).to[scala.collection.immutable.Seq]))))

  when(connector.simpleSelect(SimpleSelect(AllField, TableName(aTableName),
    where = Some(Or(Seq(
      EqualToValue(FieldName("A1"), Value("v1")),
      EqualToValue(FieldName("A1"), Value("v2")),
      IsNull(FieldName("A2"))
    )))), queryTimeout)).thenReturn(Future(Right(Source(Seq(
    Seq("A1", "A2", "A3"),
    Seq("v1", "1", "1"),
    Seq("v5", null, "0"),
    Seq("v2", "2", "0"),
    Seq("v7", null, null)
  ).to[scala.collection.immutable.Seq]))))

  when(connector.simpleSelect(SimpleSelect(AllField, TableName(aTableName),
    where = Some(And(Seq(
      EqualToValue(FieldName("A1"), Value("v7")),
      IsNull(FieldName("A2"))
    )))), queryTimeout)).thenReturn(Future(Right(Source(Seq(
    Seq("A1", "A2", "A3"),
    Seq("v7", null, null)
  ).to[scala.collection.immutable.Seq]))))

  when(connector.simpleSelect(SimpleSelect(AllField, TableName(aTableName),
    where = Some(And(Seq(
      EqualToValue(FieldName("A1"), Value("v7")),
      NotNull(FieldName("A2"))
    )))), queryTimeout)).thenReturn(Future(Right(Source.empty)))

  when(connector.simpleSelect(SimpleSelect(AllField, TableName(aTableName),
    where = Some(Or(Seq(
      EqualToValue(FieldName("A1"), Value("v1")),
      And(Seq(
        IsNull(FieldName("A2")),
        IsNull(FieldName("A3"))
      ))
    )))), queryTimeout)).thenReturn(Future(Right(Source(Seq(
    Seq("A1", "A2", "A3"),
    Seq("v1", "1", "1"),
    Seq("v7", null, null)
  ).to[scala.collection.immutable.Seq]))))
}
