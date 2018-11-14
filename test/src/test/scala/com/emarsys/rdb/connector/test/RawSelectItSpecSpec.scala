package com.emarsys.rdb.connector.test

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.models.Errors.ErrorWithMessage
import com.emarsys.rdb.connector.common.models.Connector
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.mockito.MockitoSugar

import scala.concurrent.{ExecutionContextExecutor, Future}

class RawSelectItSpecSpec extends TestKit(ActorSystem()) with RawSelectItSpec with MockitoSugar with BeforeAndAfterAll {

  implicit val materializer: Materializer = ActorMaterializer()

  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  override val connector = mock[Connector]

  override def beforeAll(): Unit = ()

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  val simpleSelect = s"""SELECT * FROM "$aTableName";"""
  val badSimpleSelect = s"""SELECT * ForM "$aTableName""""
  val simpleSelectNoSemicolon = s"""SELECT * FROM "$aTableName""""

  when(connector.rawSelect(simpleSelect, None, queryTimeout)).thenReturn(Future(Right(Source(Seq(
    Seq("A1", "A2", "A3"),
    Seq("v1", "1", "1"),
    Seq("v3", "3", "1"),
    Seq("v2", "2", "0"),
    Seq("v6", "6", null),
    Seq("v4", "-4", "0"),
    Seq("v5", null, "0"),
    Seq("v7", null, null)
  ).to[scala.collection.immutable.Seq]))))

  when(connector.rawSelect(badSimpleSelect, None, queryTimeout)).thenReturn(Future(Left(ErrorWithMessage("bad query"))))

  when(connector.rawSelect(simpleSelect, Some(2), queryTimeout)).thenReturn(Future(Right(Source(Seq(
    Seq("A1", "A2", "A3"),
    Seq("v1", "1", "1"),
    Seq("v3", "3", "1")
  ).to[scala.collection.immutable.Seq]))))

  when(connector.validateRawSelect(simpleSelect)).thenReturn(Future(Right()))

  when(connector.validateRawSelect(badSimpleSelect)).thenReturn(Future(Left(ErrorWithMessage("bad query"))))

  when(connector.validateRawSelect(simpleSelectNoSemicolon)).thenReturn(Future(Right()))

  when(connector.validateProjectedRawSelect(simpleSelect, Seq("A1"))).thenReturn(Future(Right()))

  when(connector.validateProjectedRawSelect(simpleSelectNoSemicolon, Seq("A1"))).thenReturn(Future(Right()))

  when(connector.validateProjectedRawSelect(simpleSelect, Seq("NONEXISTENT_COLUMN"))).thenReturn(Future(Left(ErrorWithMessage("bad query"))))

  when(connector.projectedRawSelect(simpleSelect, Seq("A2", "A3"), None, queryTimeout, allowNullFieldValue = false)).thenReturn(Future(Right(Source(Seq(
    Seq("A2", "A3"),
    Seq("1", "1"),
    Seq("3", "1"),
    Seq("2", "0"),
    Seq("-4", "0")
  ).to[scala.collection.immutable.Seq]))))

  when(connector.projectedRawSelect(simpleSelect, Seq("A2", "A3"), None, queryTimeout, allowNullFieldValue = true)).thenReturn(Future(Right(Source(Seq(
    Seq("A2", "A3"),
    Seq("1", "1"),
    Seq("3", "1"),
    Seq("2", "0"),
    Seq("6", null),
    Seq("-4", "0"),
    Seq(null, "0"),
    Seq(null, null)
  ).to[scala.collection.immutable.Seq]))))

  when(connector.projectedRawSelect(simpleSelect, Seq("A1"), None, queryTimeout)).thenReturn(Future(Right(Source(Seq(
    Seq("A1"),
    Seq("v1"),
    Seq("v3"),
    Seq("v2"),
    Seq("v6"),
    Seq("v4"),
    Seq("v5"),
    Seq("v7")
  ).to[scala.collection.immutable.Seq]))))


  when(connector.projectedRawSelect(simpleSelect, Seq("A1"), Some(3), queryTimeout)).thenReturn(Future(Right(Source(Seq(
    Seq("A1"),
    Seq("v1"),
    Seq("v3"),
    Seq("v2"),
  ).to[scala.collection.immutable.Seq]))))

}
