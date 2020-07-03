package com.emarsys.rdb.connector.snowflake

import java.util.UUID

import com.emarsys.rdb.connector.common.models.Errors.ErrorCategory.FatalQueryExecution
import com.emarsys.rdb.connector.common.models.Errors.ErrorName.TableNotFound
import com.emarsys.rdb.connector.snowflake.utils.{BaseDbSpec, TestHelper}
import com.emarsys.rdb.connector.test.CustomMatchers.haveErrorCategoryAndErrorName
import com.emarsys.rdb.connector.test.util.EitherValues
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._

class SnowflakeIsOptimizedSpec
    extends WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with EitherValues
    with BaseDbSpec {
  val awaitTimeout     = 5.seconds
  val awaitTimeoutLong = 15.seconds

  val uuid      = UUID.randomUUID().toString
  val tableName = s"is_optimized_table_$uuid"

  override def beforeAll(): Unit = {
    Await.result(TestHelper.executeQuery(s"""CREATE TABLE "$tableName" (A0 INT);"""), awaitTimeout)
  }

  override def afterAll(): Unit = {
    Await.result(TestHelper.executeQuery(s"""DROP TABLE "$tableName";"""), awaitTimeoutLong)
    connector.close()
  }

  "IsOptimizedSpec" when {

    "#isOptimized" should {

      "succeed" in {
        Await.result(connector.isOptimized(tableName, Seq.empty), awaitTimeout).value shouldBe true
      }

      "fail if table not found" in {
        val result = Await.result(connector.isOptimized("NON_EXISTING_TABLENAME", Seq.empty), awaitTimeout)

        result.left.value should haveErrorCategoryAndErrorName(FatalQueryExecution, TableNotFound)
      }
    }
  }
}
