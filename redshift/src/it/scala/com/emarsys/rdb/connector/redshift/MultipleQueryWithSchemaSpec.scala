package com.emarsys.rdb.connector.redshift

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl.Sink
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName}
import com.emarsys.rdb.connector.redshift.utils.SelectDbWithSchemaInitHelper
import com.emarsys.rdb.connector.test.uuidGenerate
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, WordSpecLike}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

class MultipleQueryWithSchemaSpec
    extends TestKit(ActorSystem("MultipleQueryWithSchemaSpec"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with SelectDbWithSchemaInitHelper {
  val uuid      = uuidGenerate
  val tableName = s"multiple_query_table_$uuid"

  val queryTimeout                                = 30.seconds
  val awaitTimeout                                = 90.seconds
  implicit val materializer: Materializer         = ActorMaterializer()
  implicit val executionContext: ExecutionContext = system.dispatcher

  override def beforeEach(): Unit = {
    initDb()
  }

  override def afterEach(): Unit = {
    cleanUpDb()
  }

  override def afterAll(): Unit = {
    connector.close()
    shutdown()
  }

  val aTableName: String = tableName
  val bTableName: String = s"temp_$uuid"

  s"MultipleQueryWithSchemaSpec $uuid" when {

    "run parallelly multiple query" in {

      val slowQuery = s"""SELECT A1 FROM "$aTableName";"""

      val queries = (0 until 10).map { _ =>
        connector.rawSelect(slowQuery, None, queryTimeout).flatMap {
          case Left(ex) => Future.successful(Left(ex))
          case Right(source) =>
            source.runWith(Sink.seq).map(Right(_)).recover {
              case error =>
                Left(
                  DatabaseError(
                    ErrorCategory.Unknown,
                    ErrorName.Unknown,
                    "Something went wrong",
                    Some(error),
                    None
                  )
                )
            }
        }
      }

      val results = Await.result(Future.sequence(queries), awaitTimeout)
      results.forall(_.isRight) shouldBe true
    }
  }
}
