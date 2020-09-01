package com.emarsys.rdb.connector.mysql

import java.util.UUID

import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName}
import com.emarsys.rdb.connector.mysql.utils.{BaseDbSpec, TestHelper}
import com.emarsys.rdb.connector.test.CustomMatchers.beDatabaseErrorEqualWithoutCause
import org.scalatest.{BeforeAndAfterAll, EitherValues}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.Await
import scala.concurrent.duration._

class MySqlIsOptimizedSpec extends AnyWordSpecLike with Matchers with BeforeAndAfterAll with EitherValues with BaseDbSpec {

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
      s"""CREATE TABLE `$tableName` (
         |  A0 INT,
         |  A1 varchar(100),
         |  A2 varchar(50),
         |  A3 varchar(50),
         |  A4 varchar(50),
         |  A5 varchar(50),
         |  A6 varchar(50),
         |  PRIMARY KEY(A0)
         |);""".stripMargin
    val createIndex1Sql = s"""CREATE INDEX `$index1Name` ON `$tableName` (A1, A2);"""
    val createIndex2Sql = s"""CREATE INDEX `$index2Name` ON `$tableName` (A4, A5, A6);"""

    Await.result(
      for {
        _ <- TestHelper.executeQuery(createTableSql)
        _ <- TestHelper.executeQuery(createIndex1Sql)
        _ <- TestHelper.executeQuery(createIndex2Sql)
      } yield (),
      10.seconds
    )
  }

  def cleanUpDb(): Unit = {
    val dropIndex1Sql = s"""DROP INDEX `$index1Name` ON `$tableName`;"""
    val dropIndex2Sql = s"""DROP INDEX `$index2Name` ON `$tableName`;"""
    val dropTableSql  = s"""DROP TABLE `$tableName`;"""

    Await.result(for {
      _ <- TestHelper.executeQuery(dropIndex2Sql)
      _ <- TestHelper.executeQuery(dropIndex1Sql)
      _ <- TestHelper.executeQuery(dropTableSql)
    } yield (), 10.seconds)
  }

  "#isOptimized" when {

    "hasIndex - return TRUE" should {

      "if simple index exists in its own" in {
        val resultE = Await.result(connector.isOptimized(tableName, Seq("A0")), 5.seconds)
        resultE shouldBe a[Right[_, _]]
        val result = resultE.value
        result shouldBe true
      }

      "if simple index exists in complex index as first member" in {
        val resultE = Await.result(connector.isOptimized(tableName, Seq("A1")), 5.seconds)
        resultE shouldBe a[Right[_, _]]
        val result = resultE.value
        result shouldBe true
      }

      "if complex index exists" in {
        val resultE = Await.result(connector.isOptimized(tableName, Seq("A1", "A2")), 5.seconds)
        resultE shouldBe a[Right[_, _]]
        val result = resultE.value
        result shouldBe true
      }

      "if complex index exists but in different order" in {
        val resultE = Await.result(connector.isOptimized(tableName, Seq("A2", "A1")), 5.seconds)
        resultE shouldBe a[Right[_, _]]
        val result = resultE.value
        result shouldBe true
      }
    }

    "not hasIndex - return FALSE" should {

      "if simple index does not exists at all" in {
        val resultE = Await.result(connector.isOptimized(tableName, Seq("A3")), 5.seconds)
        resultE shouldBe a[Right[_, _]]
        val result = resultE.value
        result shouldBe false
      }

      "if simple index exists in complex index but not as first member" in {
        val resultE = Await.result(connector.isOptimized(tableName, Seq("A2")), 5.seconds)
        resultE shouldBe a[Right[_, _]]
        val result = resultE.value
        result shouldBe false
      }

      "if complex index exists only as part of another complex index" in {
        val resultE = Await.result(connector.isOptimized(tableName, Seq("A4", "A5")), 5.seconds)
        resultE shouldBe a[Right[_, _]]
        val result = resultE.value
        result shouldBe false
      }
    }

    "table not exists" should {

      "fail" in {
        val table   = "TABLENAME"
        val result  = Await.result(connector.isOptimized(table, Seq("A0")), 5.seconds)
        val message = s"Table 'it-test-db.$table' doesn't exist"
        val expectedError =
          DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.TableNotFound, message, None, None)

        result.left.value should beDatabaseErrorEqualWithoutCause(expectedError)
      }
    }
  }
}
