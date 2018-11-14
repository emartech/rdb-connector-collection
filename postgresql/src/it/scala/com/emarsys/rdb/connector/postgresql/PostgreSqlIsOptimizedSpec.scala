package com.emarsys.rdb.connector.postgresql

import java.util.UUID

import com.emarsys.rdb.connector.common.models.Connector
import com.emarsys.rdb.connector.common.models.Errors.TableNotFound
import com.emarsys.rdb.connector.postgresql.utils.TestHelper
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import slick.util.AsyncExecutor

import scala.concurrent.Await
import scala.concurrent.duration._

class PostgreSqlIsOptimizedSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {
  implicit val executionContext = scala.concurrent.ExecutionContext.Implicits.global
  val awaitTimeout              = 5.seconds
  val awaitTimeoutLong          = 15.seconds

  val uuid       = UUID.randomUUID().toString
  val tableName  = s"is_optimized_table_$uuid"
  val index1Name = s"is_optimized_index1_$uuid"
  val index2Name = s"is_optimized_index2_$uuid"

  override def beforeAll(): Unit = {
    initDb()
  }

  override def afterAll(): Unit = {
    cleanUpDb()
    connector.close()
  }

  val connector: Connector = Await
    .result(PostgreSqlConnector(TestHelper.TEST_CONNECTION_CONFIG)(AsyncExecutor.default()), awaitTimeoutLong)
    .right
    .get

  def initDb(): Unit = {
    val createTableSql =
      s"""CREATE TABLE "$tableName" (
         |  A0 INT,
         |  A1 varchar(100),
         |  A2 varchar(50),
         |  A3 varchar(50),
         |  A4 varchar(50),
         |  A5 varchar(50),
         |  A6 varchar(50),
         |  PRIMARY KEY(A0)
         |);""".stripMargin

    val createIndex1Sql = s"""CREATE INDEX "$index1Name" ON "$tableName" (a1, a2);"""
    val createIndex2Sql =
      s"""CREATE INDEX "$index2Name" ON "$tableName" (a4, a5, a6);""".stripMargin

    Await.result(
      for {
        _ <- TestHelper.executeQuery(createTableSql)
        _ <- TestHelper.executeQuery(createIndex1Sql)
        _ <- TestHelper.executeQuery(createIndex2Sql)
      } yield (),
      awaitTimeout
    )
  }

  def cleanUpDb(): Unit = {
    val dropTableSql = s"""DROP TABLE "$tableName";"""
    Await.result(TestHelper.executeQuery(dropTableSql), awaitTimeoutLong)
  }

  "IsOptimizedSpec" when {

    "#isOptimized" when {

      "hasIndex - return TRUE" should {

        "if simple index exists in its own" in {
          val resultE = Await.result(connector.isOptimized(tableName, Seq("a0")), awaitTimeout)
          resultE shouldBe a[Right[_, _]]
          val result = resultE.right.get
          result shouldBe true
        }

        "if simple index exists in complex index as first member" in {
          val resultE = Await.result(connector.isOptimized(tableName, Seq("a1")), awaitTimeout)
          resultE shouldBe a[Right[_, _]]
          val result = resultE.right.get
          result shouldBe true
        }

        "if complex index exists" in {
          val resultE = Await.result(connector.isOptimized(tableName, Seq("a1", "a2")), awaitTimeout)
          resultE shouldBe a[Right[_, _]]
          val result = resultE.right.get
          result shouldBe true
        }

        "if complex index exists but in different order" in {
          val resultE = Await.result(connector.isOptimized(tableName, Seq("a2", "a1")), awaitTimeout)
          resultE shouldBe a[Right[_, _]]
          val result = resultE.right.get
          result shouldBe true
        }
      }

      "not hasIndex - return FALSE" should {

        "if simple index does not exists at all" in {
          val resultE = Await.result(connector.isOptimized(tableName, Seq("a3")), awaitTimeout)
          resultE shouldBe a[Right[_, _]]
          val result = resultE.right.get
          result shouldBe false
        }

        "if simple index exists in complex index but not as first member" in {
          val resultE = Await.result(connector.isOptimized(tableName, Seq("a2")), awaitTimeout)
          resultE shouldBe a[Right[_, _]]
          val result = resultE.right.get
          result shouldBe false
        }

        "if complex index exists only as part of another complex index" in {
          val resultE = Await.result(connector.isOptimized(tableName, Seq("a4", "a5")), awaitTimeout)
          resultE shouldBe a[Right[_, _]]
          val result = resultE.right.get
          result shouldBe false
        }
      }

      "table not exists" should {

        "fail" in {
          val result = Await.result(connector.isOptimized("TABLENAME", Seq("a0")), awaitTimeout)
          result shouldEqual Left(TableNotFound("TABLENAME"))
        }
      }
    }
  }
}
