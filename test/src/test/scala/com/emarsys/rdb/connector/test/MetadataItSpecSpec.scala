package com.emarsys.rdb.connector.test

import com.emarsys.rdb.connector.common.models.Connector
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName}
import com.emarsys.rdb.connector.common.models.TableSchemaDescriptors.{FieldModel, FullTableModel, TableModel}
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.Future

class MetadataItSpecSpec extends MetadataItSpec with MockitoSugar {

  //case insensitive tests
  //table list not exact matches
  //works as expected

  implicit val executionContext = scala.concurrent.ExecutionContext.global

  override val connector = mock[Connector]

  override def initDb(): Unit = ()

  override def cleanUpDb(): Unit = ()

  val tableMissingMessage = s"Table 'it-test-db.TABLENAME' doesn't exist"
  val tableNotFoundError =
    DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.TableNotFound, tableMissingMessage, None, None)

  when(connector.listTables()).thenReturn(
    Future(
      Right(
        Seq(
          TableModel("asdf", false),
          TableModel(viewName, true),
          TableModel(tableName, false),
          TableModel("fghy", true)
        )
      )
    )
  )
  when(connector.listFields(tableName)).thenReturn(
    Future(
      Right(
        Seq(
          FieldModel("PersonID", "PersonIDType"),
          FieldModel("lastname", "LastNameType"),
          FieldModel("FirstName", "FirstNameType"),
          FieldModel("ADDRESS", "AddressType"),
          FieldModel("City", "")
        )
      )
    )
  )
  when(connector.listFields(viewName)).thenReturn(
    Future(
      Right(
        Seq(
          FieldModel("PersonID", "PersonIDType"),
          FieldModel("lastname", "LastNameType"),
          FieldModel("FirstName", "FirstNameType")
        )
      )
    )
  )
  when(connector.listFields("TABLENAME")).thenReturn(Future(Left(tableNotFoundError)))
  when(connector.listTablesWithFields()).thenReturn(
    Future(
      Right(
        Seq(
          FullTableModel("asdf", false, Seq()),
          FullTableModel(
            viewName,
            true,
            Seq(
              FieldModel("PersonID", "PersonIDType"),
              FieldModel("FirstName", "FirstNameType"),
              FieldModel("lastname", "LastNameType")
            )
          ),
          FullTableModel(
            tableName,
            false,
            Seq(
              FieldModel("PersonID", "PersonIDType"),
              FieldModel("FirstName", "FirstNameType"),
              FieldModel("lastname", "LastNameType"),
              FieldModel("ADDRESS", "AddressType"),
              FieldModel("City", "")
            )
          ),
          FullTableModel("fghy", true, Seq())
        )
      )
    )
  )

}
