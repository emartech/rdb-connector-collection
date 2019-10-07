package com.emarsys.rdb.connector.test

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.models.Connector
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorName, Fields}
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfterAll
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.Future

class ReplaceSpecSpec
    extends TestKit(ActorSystem("ReplaceSpecSpec"))
    with ReplaceItSpec
    with MockitoSugar
    with BeforeAndAfterAll {

  import com.emarsys.rdb.connector.utils.TestHelper._

  implicit val materializer: Materializer = ActorMaterializer()

  implicit val executionContext = system.dispatcher

  override val connector = mock[Connector]

  override def initDb(): Unit = ()

  override def cleanUpDb(): Unit = ()

  override def afterAll = {
    shutdown()
  }

  when(connector.replaceData(tableName, replaceMultipleData)).thenReturn(Future.successful(Right(3)))
  when(connector.replaceData(tableName, replaceSingleData)).thenReturn(Future.successful(Right(1)))
  when(connector.replaceData(tableName, Seq.empty)).thenReturn(Future.successful(Right(0)))
  when(connector.replaceData(tableName, replaceNonExistingFieldFieldData))
    .thenReturn(Future.successful(Left(DatabaseError.validation(ErrorName.MissingFields, Some(Fields(List("a")))))))

  Seq(simpleSelect, simpleSelectF, simpleSelectT, simpleSelectT2).foreach(
    selectUniqueValueMock(_, connector, queryTimeout)
  )
  Seq(0, 2, 4).foreach(selectExactNumberMock(_, tableName, connector, queryTimeout))

}
