package com.emarsys.rdb.connector.mssql

import java.util.UUID

import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName}
import com.emarsys.rdb.connector.mssql.utils.{BaseDbSpec, TestHelper}
import com.emarsys.rdb.connector.test.CustomMatchers.beDatabaseErrorEqualWithoutCause
import org.scalatest.{BeforeAndAfterAll, EitherValues}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.Await

class MsSqlIsOptimizedSpec extends AnyWordSpecLike with Matchers with BeforeAndAfterAll with EitherValues with BaseDbSpec {

  val uuid = UUID.randomUUID().toString

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

  def initDb(): Unit = {
    val createTableSql =
      s"""CREATE TABLE [$tableName] (
         |  A0 INT,
         |  A1 varchar(100),
         |  A2 varchar(50),
         |  A3 varchar(50),
         |  A4 varchar(50),
         |  A5 varchar(50),
         |  A6 varchar(50),
         |  PRIMARY KEY(A0)
         |);""".stripMargin
    val createIndex1Sql = s"""CREATE INDEX [$index1Name] ON [$tableName] (A1, A2);"""
    val createIndex2Sql = s"""CREATE INDEX [$index2Name] ON [$tableName] (A4, A5, A6);"""

    Await.result(
      for {
        _ <- TestHelper.executeQuery(createTableSql)
        _ <- TestHelper.executeQuery(createIndex1Sql)
        _ <- TestHelper.executeQuery(createIndex2Sql)
      } yield (),
      timeout
    )
  }

  def cleanUpDb(): Unit = {
    val dropIndex1Sql = s"""DROP INDEX [$index1Name] ON [$tableName];"""
    val dropIndex2Sql = s"""DROP INDEX [$index2Name] ON [$tableName];"""
    val dropTableSql  = s"""DROP TABLE [$tableName];"""

    Await.result(for {
      _ <- TestHelper.executeQuery(dropIndex2Sql)
      _ <- TestHelper.executeQuery(dropIndex1Sql)
      _ <- TestHelper.executeQuery(dropTableSql)
    } yield (), timeout)
  }

  "#isOptimized" when {

    "hasIndex - return TRUE" should {

      "if simple index exists in its own" in {
        val resultE = Await.result(connector.isOptimized(tableName, Seq("A0")), timeout)

        resultE shouldBe Right(true)
      }

      "if simple index exists in complex index as first member" in {
        val resultE = Await.result(connector.isOptimized(tableName, Seq("A1")), timeout)

        resultE shouldBe Right(true)
      }

      "if complex index exists" in {
        val resultE = Await.result(connector.isOptimized(tableName, Seq("A1", "A2")), timeout)

        resultE shouldBe Right(true)
      }

      "if complex index exists but in different order" in {
        val resultE = Await.result(connector.isOptimized(tableName, Seq("A2", "A1")), timeout)

        resultE shouldBe Right(true)
      }
    }

    "not hasIndex - return FALSE" should {

      "if simple index does not exists at all" in {
        val resultE = Await.result(connector.isOptimized(tableName, Seq("A3")), timeout)

        resultE shouldBe Right(false)
      }

      "if simple index exists in complex index but not as first member" in {
        val resultE = Await.result(connector.isOptimized(tableName, Seq("A2")), timeout)

        resultE shouldBe Right(false)
      }

      "if complex index exists only as part of another complex index" in {
        val resultE = Await.result(connector.isOptimized(tableName, Seq("A4", "A5")), timeout)

        resultE shouldBe Right(false)
      }
    }

    "table not exists" should {

      "fail" in {
        val table = "TABLENAME"
        val expectedError =
          DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.TableNotFound, table, None, None)
        val result = Await.result(connector.isOptimized(table, Seq("A0")), timeout)

        result.left.value should beDatabaseErrorEqualWithoutCause(expectedError)
      }
    }
  }
}
