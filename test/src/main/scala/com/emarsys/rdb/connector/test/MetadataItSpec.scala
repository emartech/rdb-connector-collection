package com.emarsys.rdb.connector.test

import com.emarsys.rdb.connector.common.models.Connector
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName}
import com.emarsys.rdb.connector.common.models.TableSchemaDescriptors.{FieldModel, FullTableModel, TableModel}
import com.emarsys.rdb.connector.test.CustomMatchers.beDatabaseErrorEqualWithoutCause
import com.emarsys.rdb.connector.test.util.EitherValues
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._

/*
For positive test results you need to implement an initDb function which creates a table and a view with the given names.
The table must have "PersonID", "LastName", "FirstName", "Address", "City" columns.
The view must have "PersonID", "LastName", "FirstName" columns.
In addition, if the database supports it, a table with the given name should be created in a schema different from the default with an arbitrary schema.
 */
trait MetadataItSpec extends WordSpecLike with Matchers with BeforeAndAfterAll with EitherValues {
  val uuid      = uuidGenerate
  val otherSchema = s"other_schema_$uuid"
  val tableName = s"metadata_list_tables_table_$uuid"
  val tableNameInOtherSchema = s"mlt_other_schema_$uuid" // name has to be short, otherwise it might get truncated in postgres
  val viewName  = s"metadata_list_tables_view_$uuid"
  val connector: Connector
  val awaitTimeout = 10.seconds

  override def beforeAll(): Unit = {
    initDb()
  }

  override def afterAll(): Unit = {
    cleanUpDb()
    connector.close()
  }

  def initDb(): Unit
  def cleanUpDb(): Unit

  s"MetadataItSpec $uuid" when {

    "#listTables" should {
      "list tables and views" in {
        val resultE = Await.result(connector.listTables(), awaitTimeout)

        resultE shouldBe a[Right[_, _]]
        val result = resultE.value

        result should contain(TableModel(tableName, false))
        result should contain(TableModel(viewName, true))
        result should not contain(TableModel(tableNameInOtherSchema, false))
      }
    }

    "#listFields" should {
      "list table fields" in {
        val tableFields =
          Seq("PersonID", "LastName", "FirstName", "Address", "City").map(_.toLowerCase()).sorted.map(FieldModel(_, ""))

        val resultE = Await.result(connector.listFields(tableName), awaitTimeout)

        resultE shouldBe a[Right[_, _]]
        val result = resultE.value

        val fieldModels = result.map(f => f.copy(name = f.name.toLowerCase, columnType = "")).sortBy(_.name)

        fieldModels shouldBe tableFields
      }

      "failed if table not found" in {
        val table  = "TABLENAME"
        val result = Await.result(connector.listFields(table), awaitTimeout)
        val expectedError =
          DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.TableNotFound, table, None, None)

        result.left.value should beDatabaseErrorEqualWithoutCause(expectedError)
      }
    }

    "#listTablesWithFields" should {
      "list all" in {
        val tableFields =
          Seq("PersonID", "LastName", "FirstName", "Address", "City").map(_.toLowerCase()).sorted.map(FieldModel(_, ""))
        val viewFields = Seq("PersonID", "LastName", "FirstName").map(_.toLowerCase()).sorted.map(FieldModel(_, ""))

        val resultE = Await.result(connector.listTablesWithFields(), awaitTimeout)

        resultE shouldBe a[Right[_, _]]
        val result = resultE.value.map(x =>
          x.copy(fields = x.fields.map(f => f.copy(name = f.name.toLowerCase, columnType = "")).sortBy(_.name))
        )

        result should contain(FullTableModel(tableName, false, tableFields))
        result should contain(FullTableModel(viewName, true, viewFields))
      }
    }
  }
}
