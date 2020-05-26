package com.emarsys.rdb.connector.test

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl.Source
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.models.Connector
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName}
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterAll
import org.scalatestplus.mockito.MockitoSugar


import scala.collection.immutable //TODO remove this after 2.13
import scala.concurrent.{ExecutionContextExecutor, Future}

class RawSelectItSpecSpec
    extends TestKit(ActorSystem("RawSelectItSpecSpec"))
    with RawSelectItSpec
    with MockitoSugar
    with BeforeAndAfterAll {

  implicit val materializer: Materializer = ActorMaterializer()

  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  override val connector = mock[Connector]

  override def beforeAll(): Unit = ()

  override def afterAll = {
    shutdown()
  }

  val simpleSelect    = s"""SELECT * FROM "$aTableName";"""
  val badSimpleSelect = s"""SELECT * ForM "$aTableName""""
  val databaseError   = DatabaseError(ErrorCategory.Unknown, ErrorName.Unknown, "oh no")

  val simpleSelectNoSemicolon = s"""SELECT * FROM "$aTableName""""
  when(connector.rawSelect(simpleSelect, None, queryTimeout)).thenReturn(
    Future(
      Right(
        Source(
          immutable.Seq(
            immutable.Seq("A1", "A2", "A3"),
            immutable.Seq("v1", "1", "1"),
            immutable.Seq("v3", "3", "1"),
            immutable.Seq("v2", "2", "0"),
            immutable.Seq("v6", "6", null),
            immutable.Seq("v4", "-4", "0"),
            immutable.Seq("v5", null, "0"),
            immutable.Seq("v7", null, null)
          )
        )
      )
    )
  )

  when(connector.rawSelect(badSimpleSelect, None, queryTimeout))
    .thenReturn(Future(Left(databaseError)))

  when(connector.rawSelect(simpleSelect, Some(2), queryTimeout)).thenReturn(
    Future(
      Right(
        Source(
          immutable.Seq(
            immutable.Seq("A1", "A2", "A3"),
            immutable.Seq("v1", "1", "1"),
            immutable.Seq("v3", "3", "1")
          )
        )
      )
    )
  )

  when(connector.validateRawSelect(simpleSelect)).thenReturn(Future(Right(())))

  when(connector.validateRawSelect(badSimpleSelect))
    .thenReturn(Future(Left(databaseError)))

  when(connector.validateRawSelect(simpleSelectNoSemicolon)).thenReturn(Future(Right(())))

  when(connector.validateProjectedRawSelect(simpleSelect, immutable.Seq("A1"))).thenReturn(Future(Right(())))

  when(connector.validateProjectedRawSelect(simpleSelectNoSemicolon, immutable.Seq("A1"))).thenReturn(Future(Right(())))

  when(connector.validateProjectedRawSelect(simpleSelect, immutable.Seq("NONEXISTENT_COLUMN")))
    .thenReturn(Future(Left(databaseError)))

  when(connector.projectedRawSelect(simpleSelect, immutable.Seq("A2", "A3"), None, queryTimeout, allowNullFieldValue = false))
    .thenReturn(
      Future(
        Right(
          Source(
            immutable.Seq(
              immutable.Seq("A2", "A3"),
              immutable.Seq("1", "1"),
              immutable.Seq("3", "1"),
              immutable.Seq("2", "0"),
              immutable.Seq("-4", "0")
            )
          )
        )
      )
    )

  when(connector.projectedRawSelect(simpleSelect, immutable.Seq("A2", "A3"), None, queryTimeout, allowNullFieldValue = true))
    .thenReturn(
      Future(
        Right(
          Source(
            immutable.Seq(
              immutable.Seq("A2", "A3"),
              immutable.Seq("1", "1"),
              immutable.Seq("3", "1"),
              immutable.Seq("2", "0"),
              immutable.Seq("6", null),
              immutable.Seq("-4", "0"),
              immutable.Seq(null, "0"),
              immutable.Seq(null, null)
            )
          )
        )
      )
    )

  when(connector.projectedRawSelect(simpleSelect, immutable.Seq("A1"), None, queryTimeout)).thenReturn(
    Future(
      Right(
        Source(
          immutable.Seq(
            immutable.Seq("A1"),
            immutable.Seq("v1"),
            immutable.Seq("v3"),
            immutable.Seq("v2"),
            immutable.Seq("v6"),
            immutable.Seq("v4"),
            immutable.Seq("v5"),
            immutable.Seq("v7")
          )
        )
      )
    )
  )

  when(connector.projectedRawSelect(simpleSelect, immutable.Seq("A1"), Some(3), queryTimeout)).thenReturn(
    Future(
      Right(
        Source(
          immutable.Seq(
            immutable.Seq("A1"),
            immutable.Seq("v1"),
            immutable.Seq("v3"),
            immutable.Seq("v2")
          )
        )
      )
    )
  )

}
