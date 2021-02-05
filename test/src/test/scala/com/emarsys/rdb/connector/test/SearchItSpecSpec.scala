package com.emarsys.rdb.connector.test

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.models.Connector
import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper.{BooleanValue, IntValue, NullValue, StringValue}
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterAll
import org.scalatestplus.mockito.MockitoSugar

import scala.collection.immutable
import scala.concurrent.Future

class SearchItSpecSpec
    extends TestKit(ActorSystem("SearchItSpecSpec"))
    with SearchItSpec
    with MockitoSugar
    with BeforeAndAfterAll {

  implicit val executionContext = system.dispatcher

  override val connector = mock[Connector]

  override def initDb(): Unit = ()

  override def cleanUpDb(): Unit = ()

  override def afterAll() = {
    shutdown()
  }

  when(connector.search(tableName, Map("z1" -> StringValue("r1")), None, queryTimeout)).thenReturn(
    Future(
      Right(
        Source(
          immutable.Seq(
            immutable.Seq("Z1", "Z2", "Z3", "Z4"),
            immutable.Seq("r1", "1", "1", "s1")
          )
        )
      )
    )
  )

  when(connector.search(tableName, Map("z2" -> IntValue(2)), None, queryTimeout)).thenReturn(
    Future(
      Right(
        Source(
          immutable.Seq(
            immutable.Seq("Z1", "Z2", "Z3", "Z4"),
            immutable.Seq("r2", "2", "0", "s2")
          )
        )
      )
    )
  )

  when(connector.search(tableName, Map("z3" -> BooleanValue(false)), None, queryTimeout)).thenReturn(
    Future(
      Right(
        Source(
          immutable.Seq(
            immutable.Seq("Z1", "Z2", "Z3", "Z4"),
            immutable.Seq("r2", "2", "0", "s2")
          )
        )
      )
    )
  )

  when(connector.search(tableName, Map("z3" -> NullValue), None, queryTimeout)).thenReturn(
    Future(
      Right(
        Source(
          immutable.Seq(
            immutable.Seq("Z1", "Z2", "Z3", "Z4"),
            immutable.Seq("r3", "3", null, "s3")
          )
        )
      )
    )
  )

  when(connector.search(tableName, Map("z2" -> IntValue(45)), None, queryTimeout)).thenReturn(
    Future(
      Right(
        Source(
          immutable.Seq(
            immutable.Seq("Z1", "Z2", "Z3", "Z4"),
            immutable.Seq("r4", "45", "1", "s4"),
            immutable.Seq("r5", "45", "1", "s5")
          )
        )
      )
    )
  )

}
