package com.emarsys.rdb.connector.test

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.models.Connector
import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper.{BooleanValue, IntValue, NullValue, StringValue}
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.mockito.MockitoSugar

import scala.concurrent.Future

class SearchItSpecSpec extends TestKit(ActorSystem()) with SearchItSpec with MockitoSugar with BeforeAndAfterAll {

  implicit val materializer: Materializer = ActorMaterializer()

  implicit val executionContext = system.dispatcher

  override val connector = mock[Connector]

  override def initDb(): Unit = ()

  override def cleanUpDb(): Unit = ()

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  when(connector.search(tableName, Map("z1" -> StringValue("r1")), None, queryTimeout)).thenReturn(Future(Right(Source(Seq(
    Seq("Z1", "Z2", "Z3", "Z4"),
    Seq("r1", "1", "1", "s1")
  ).to[scala.collection.immutable.Seq]))))

  when(connector.search(tableName, Map("z2" -> IntValue(2)), None, queryTimeout)).thenReturn(Future(Right(Source(Seq(
    Seq("Z1", "Z2", "Z3", "Z4"),
    Seq("r2", "2", "0", "s2")
  ).to[scala.collection.immutable.Seq]))))

  when(connector.search(tableName, Map("z3" -> BooleanValue(false)), None, queryTimeout)).thenReturn(Future(Right(Source(Seq(
    Seq("Z1", "Z2", "Z3", "Z4"),
    Seq("r2", "2", "0", "s2")
  ).to[scala.collection.immutable.Seq]))))

  when(connector.search(tableName, Map("z3" -> NullValue), None, queryTimeout)).thenReturn(Future(Right(Source(Seq(
    Seq("Z1", "Z2", "Z3", "Z4"),
    Seq("r3", "3", null, "s3")
  ).to[scala.collection.immutable.Seq]))))

  when(connector.search(tableName, Map("z2" -> IntValue(45)), None, queryTimeout)).thenReturn(Future(Right(Source(Seq(
    Seq("Z1", "Z2", "Z3", "Z4"),
    Seq("r4", "45", "1", "s4"),
    Seq("r5", "45", "1", "s5")
  ).to[scala.collection.immutable.Seq]))))

}
