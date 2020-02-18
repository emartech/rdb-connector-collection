package com.emarsys.rdb.connector.redshift

import java.util.UUID

import com.emarsys.rdb.connector.common.models.Errors.{ErrorCategory, ErrorName}
import com.emarsys.rdb.connector.redshift.utils.{BaseDbSpec, TestHelper}
import com.emarsys.rdb.connector.test.CustomMatchers._
import org.scalatest.{BeforeAndAfterAll, EitherValues, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._

class RedshiftIsOptimizedSpec
    extends WordSpecLike
    with EitherValues
    with Matchers
    with BeforeAndAfterAll
    with BaseDbSpec {
  val uuid      = UUID.randomUUID().toString
  val tableName = s"is_optimized_table_$uuid"

  override def beforeAll(): Unit = {
    initDb()
  }

  override def afterAll(): Unit = {
    cleanUpDb()
    connector.close()
  }

  def initDb(): Unit = {
    val createTableSql =
      s"""CREATE TABLE "$tableName" (
         |    PersonID int,
         |    LastName varchar(255),
         |    FirstName varchar(255),
         |    Address varchar(255),
         |    City varchar(255)
         |);""".stripMargin

    Await.result(for {
      _ <- TestHelper.executeQuery(createTableSql)
    } yield (), 15.seconds)
  }

  def cleanUpDb(): Unit = {
    val dropTableSql = s"""DROP TABLE "$tableName";"""
    Await.result(TestHelper.executeQuery(dropTableSql), 15.seconds)
  }

  "IsOptimizedSpec" when {

    "#isOptimized" should {

      "success" in {
        val result = Await.result(connector.isOptimized(tableName, Seq("ANY")), 15.seconds)
        result shouldBe Right(true)
      }

      "failed if table not found" in {
        val result = Await.result(connector.isOptimized("TABLENAME", Seq("ANY")), 15.seconds)
        result.left.value should haveErrorCategoryAndErrorName(
          ErrorCategory.FatalQueryExecution,
          ErrorName.TableNotFound
        )
      }

    }
  }
}
