package com.emarsys.rbd.connector.bigquery

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import akka.util.Timeout
import com.emarsys.rbd.connector.bigquery.utils.{DbInitUtil, TestHelper}
import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper.StringValue
import com.emarsys.rdb.connector.common.models.Errors.NotImplementedOperation
import com.emarsys.rdb.connector.test.uuidGenerate
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._

class BigQuerySearchItSpec extends TestKit(ActorSystem()) with WordSpecLike with Matchers with BeforeAndAfterAll with DbInitUtil {

  override implicit val sys: ActorSystem = system
  override implicit val materializer: ActorMaterializer = ActorMaterializer()
  override implicit val timeout: Timeout = 10.seconds
  val awaitTimeout = timeout.duration

  val uuid = uuidGenerate
  val tableName = s"search_table_$uuid"

  override def beforeAll(): Unit = {
    initTable()
  }

  override def afterAll(): Unit = {
    Await.result(runRequest(dropTable(tableName)), timeout.duration)
    sleep()
    connector.close()
    shutdown()
  }

  s"BigQuerySearchIt $uuid" should {

    "return NotImplementedOperation" in {
      val result = Await.result(connector.search(tableName, Map("C" -> StringValue("c12")), None), awaitTimeout)
      result shouldEqual Left(NotImplementedOperation)
    }
  }

  private def initTable(): Unit = {
    val createTableSql =
      s"""{
         |  "friendlyName": "$tableName",
         |  "tableReference": {
         |    "datasetId": "${TestHelper.TEST_CONNECTION_CONFIG.dataset}",
         |    "projectId": "${TestHelper.TEST_CONNECTION_CONFIG.projectId}",
         |    "tableId": "$tableName"
         |  },
         |  "schema": {
         |    "fields": [
         |      {
         |        "name": "C",
         |        "type": "STRING",
         |        "mode": "REQUIRED"
         |      }
         |    ]
         |  }
         |}
       """.stripMargin

    val insertDataSql =
      """
        |{
        |  rows: [
        |    {
        |      "json": {
        |        "C": "c12",
        |      }
        |    },
        |    {
        |      "json": {
        |        "C": "c12",
        |      }
        |    },
        |    {
        |      "json": {
        |        "C": "c3",
        |      }
        |    }
        |  ]
        |}
      """.stripMargin

    Await.result(for {
      _ <- runRequest(createTable(createTableSql))
      _ <- runRequest(insertInto(insertDataSql, tableName))
    } yield (), timeout.duration)
    sleep()
  }
}
