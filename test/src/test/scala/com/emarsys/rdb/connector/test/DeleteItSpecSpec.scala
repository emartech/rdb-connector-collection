package com.emarsys.rdb.connector.test

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.models.Connector
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfterAll
import org.scalatest.mockito.MockitoSugar

import scala.concurrent.Future

class DeleteItSpecSpec extends TestKit(ActorSystem()) with DeleteItSpec with MockitoSugar with BeforeAndAfterAll {

  import com.emarsys.rdb.connector.utils.TestHelper._

  implicit val materializer: Materializer = ActorMaterializer()

  implicit val executionContext = system.dispatcher

  override val connector = mock[Connector]

  override def initDb(): Unit = ()

  override def cleanUpDb(): Unit = ()

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  when(connector.delete(tableName, simpleDeleteCiterion)).thenReturn(Future.successful(Right(1)))
  when(connector.delete(tableName, complexDeleteCriterion)).thenReturn(Future.successful(Right(2)))
  when(connector.delete(tableName, nullValueDeleteCriterion)).thenReturn(Future.successful(Right(2)))
  when(connector.delete(tableName, notMatchingComplexDeleteCriterion)).thenReturn(Future.successful(Right(0)))
  when(connector.delete(tableName, Seq.empty)).thenReturn(Future.successful(Right(0)))


  Seq(simpleNullSelect, simpleSelect, complexSelect).foreach(emptyResultSelectMock(_, connector, queryTimeout))
  Seq(5, 6, 7, 8).foreach(selectExactNumberMock(_, tableName, connector, queryTimeout))

}
