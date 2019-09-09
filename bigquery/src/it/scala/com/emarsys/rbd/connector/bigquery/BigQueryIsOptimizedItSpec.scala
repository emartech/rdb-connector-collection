package com.emarsys.rbd.connector.bigquery

import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import akka.util.Timeout
import com.emarsys.rbd.connector.bigquery.utils.MetaDbInitHelper
import com.emarsys.rdb.connector.common.models.Errors.{ErrorCategory, ErrorName}
import com.emarsys.rdb.connector.test.CustomMatchers._
import org.scalatest.{AsyncWordSpecLike, BeforeAndAfterAll, EitherValues, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

class BigQueryIsOptimizedItSpec
    extends TestKit(ActorSystem("BigQueryIsOptimizedItSpec"))
    with AsyncWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with MetaDbInitHelper
    with EitherValues {

  implicit override val sys: ActorSystem                = system
  implicit override val materializer: ActorMaterializer = ActorMaterializer()
  implicit override val timeout: Timeout                = Timeout(30.second)
  implicit val asyncWordSpecEC: ExecutionContext        = executionContext

  val uuid      = UUID.randomUUID().toString.replace("-", "")
  val tableName = s"is_optimized_table_$uuid"

  override val viewName: String = ""

  override def beforeAll(): Unit = {
    Await.result(runRequest(createTable(createTableSql)), timeout.duration)
    sleep()
  }

  override def afterAll(): Unit = {
    Await.result(runRequest(dropTable(tableName)), timeout.duration)
    shutdown()
  }

  "IsOptimizedSpec" when {

    "#isOptimized" should {

      "succeed" in {
        connector.isOptimized(tableName, Seq.empty).map { result =>
          result.right.value shouldBe true
        }
      }

      "fail if table not found" in {
        connector.isOptimized("NON_EXISTING_TABLENAME", Seq.empty) map { result =>
          result.left.value should haveErrorCategoryAndErrorName(
            ErrorCategory.FatalQueryExecution,
            ErrorName.TableNotFound
          )
        }
      }
    }
  }
}
