package com.emarsys.rbd.connector.bigquery

import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import akka.util.Timeout
import com.emarsys.rbd.connector.bigquery.utils.MetaDbInitHelper
import com.emarsys.rdb.connector.common.models.Errors.TableNotFound
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._

class BigQueryIsOptimizedItSpec extends TestKit(ActorSystem()) with WordSpecLike with Matchers with BeforeAndAfterAll with MetaDbInitHelper {

  implicit override val sys: ActorSystem = system
  implicit override val materializer: ActorMaterializer = ActorMaterializer()
  implicit override val timeout: Timeout = Timeout(30.second)

  val awaitTimeout = 30.seconds

  val uuid = UUID.randomUUID().toString.replace("-", "")
  val tableName = s"is_optimized_table_$uuid"

  override val viewName: String = ""

  override def initDb(): Unit = {
    Await.result(for {
      _ <- runRequest(createTable(createTableSql))
    } yield (), timeout.duration)
  }

  override def cleanUpDb(): Unit = {
    Await.result(for {
      _ <- runRequest(dropTable(tableName))
    } yield (), timeout.duration)
  }

  override def beforeAll(): Unit = {
    initDb()
  }

  override def afterAll(): Unit = {
    cleanUpDb()
    connector.close()
    shutdown()
  }

  "IsOptimizedSpec" when {

    "#isOptimized" should {

      "success" in {
        val result = Await.result(connector.isOptimized(tableName, Seq("ANY")), awaitTimeout)
        result shouldBe Right(true)
      }

      "failed if table not found" in {
        val result = Await.result(connector.isOptimized("TABLENAME", Seq("ANY")), awaitTimeout)
        result shouldBe Left(TableNotFound("TABLENAME"))
      }

    }
  }

}
