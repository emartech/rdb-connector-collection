package com.emarsys.rdb.connector.postgresql

import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.models.Errors.{QueryTimeout, SqlSyntaxError}
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import com.emarsys.rdb.connector.common.models.{Errors, SimpleSelect}
import com.emarsys.rdb.connector.postgresql.utils.SelectDbInitHelper
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._

class PostgreSqlRawQueryItSpec
    extends TestKit(ActorSystem())
    with SelectDbInitHelper
    with WordSpecLike
    with Matchers
    with BeforeAndAfterEach
    with BeforeAndAfterAll {

  val uuid = UUID.randomUUID().toString.replace("-", "")

  val aTableName: String = s"raw_query_tables_table_$uuid"
  val bTableName: String = s"temp_$uuid"

  implicit val materializer: Materializer = ActorMaterializer()

  val awaitTimeout = 10.seconds
  val queryTimeout = 5.seconds

  override def afterAll(): Unit = {
    system.terminate()
    connector.close()
  }

  override def beforeEach(): Unit = {
    initDb()
  }

  override def afterEach(): Unit = {
    cleanUpDb()
  }

  s"RawQuerySpec $uuid" when {

    "#rawQuery" should {

      "validation error" in {
        val invalidSql = "invalid sql"
        val result = Await.result(connector.rawQuery(invalidSql, queryTimeout), awaitTimeout)

        result.left.get shouldBe a[SqlSyntaxError]
      }

      "run a delete query" in {
        Await.result(connector.rawQuery(s"DELETE FROM $aTableName WHERE A1!='v1'", queryTimeout), awaitTimeout)
        selectAll(aTableName) shouldEqual Right(Vector(Vector("v1", "1", "1")))
      }

      "return SqlSyntaxError when select query given" in {
        val result: Either[Errors.ConnectorError, Int] =
          Await.result(connector.rawQuery(s"SELECT 1;", queryTimeout), awaitTimeout)
        result should be('left)
        result.left.get shouldBe SqlSyntaxError("Wrong update statement: non update query given")
      }

      "return QueryTimeout when query takes more time than the timeout" in {
        val query  = s"DELETE FROM $aTableName WHERE A1 = pg_sleep(12)::text"
        val result = Await.result(connector.rawQuery(query, 1.second), awaitTimeout)

        result.left.get shouldBe a[QueryTimeout]
      }

    }
  }

  private def selectAll(tableName: String) = {
    Await
      .result(connector.simpleSelect(SimpleSelect(AllField, TableName(tableName)), queryTimeout), awaitTimeout)
      .map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).drop(1))
  }
}
