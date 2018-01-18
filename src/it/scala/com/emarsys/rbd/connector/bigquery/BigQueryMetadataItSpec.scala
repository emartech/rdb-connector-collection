package com.emarsys.rbd.connector.bigquery

import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import akka.util.Timeout
import com.emarsys.rbd.connector.bigquery.utils.MetaDbInitHelper
import com.emarsys.rdb.connector.common.models.Errors.TableNotFound
import com.emarsys.rdb.connector.common.models.TableSchemaDescriptors.{FieldModel, FullTableModel, TableModel}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._

class BigQueryMetadataItSpec extends TestKit(ActorSystem()) /*with SimpleSelectItSpec*/ with WordSpecLike with Matchers with BeforeAndAfterAll with MetaDbInitHelper {

  implicit override val sys: ActorSystem = system
  implicit override val materializer: ActorMaterializer = ActorMaterializer()
  implicit override val timeout: Timeout = Timeout(30.second)

  val uuid = UUID.randomUUID().toString.replace("-", "")
  val tableName = s"metadata_list_tables_table_$uuid"
  val viewName = s"metadata_list_tables_view_$uuid"
  val awaitTimeout = 30.seconds

  override def beforeAll(): Unit = {
    initDb()
  }

  override def afterAll(): Unit = {
    cleanUpDb()
    connector.close()
    shutdown()
  }


  s"MetadataItSpec $uuid" when {

    "#listTables" should {
      "list tables and views" in {
        val resultE = Await.result(connector.listTables(), awaitTimeout)

        resultE shouldBe a[Right[_, _]]
        val result = resultE.right.get

        result should contain(TableModel(tableName, false))
        result should contain(TableModel(viewName, true))
      }
    }

    "#listFields" should {
      "list table fields" in {
        val tableFields = Seq("PersonID", "LastName", "FirstName", "Address", "City").map(_.toLowerCase()).sorted.map(FieldModel(_, ""))

        val resultE = Await.result(connector.listFields(tableName), awaitTimeout)

        resultE shouldBe a[Right[_, _]]
        val result = resultE.right.get

        val fieldModels = result.map(f => f.copy(name = f.name.toLowerCase, columnType = "")).sortBy(_.name)

        fieldModels shouldBe tableFields
      }

      "failed if table not found" in {
        val result = Await.result(connector.listFields("TABLENAME"), awaitTimeout)
        result shouldBe Left(TableNotFound("TABLENAME"))
      }
    }

    "#listTablesWithFields" should {
      "list all" in {
        val tableFields = Seq("PersonID", "LastName", "FirstName", "Address", "City").map(_.toLowerCase()).sorted.map(FieldModel(_, ""))
        val viewFields = Seq("PersonID", "LastName", "FirstName").map(_.toLowerCase()).sorted.map(FieldModel(_, ""))

        val resultE = Await.result(connector.listTablesWithFields(), awaitTimeout)

        resultE shouldBe a[Right[_, _]]
        val result = resultE.right.get.map(x => x.copy(fields = x.fields.map(f => f.copy(name = f.name.toLowerCase, columnType = "")).sortBy(_.name)))


        result should contain(FullTableModel(tableName, false, tableFields))
        result should contain(FullTableModel(viewName, true, viewFields))
      }
    }
  }
}
