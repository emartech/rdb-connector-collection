package com.emarsys.rdb.connector.bigquery

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, HttpRequest}
import akka.stream.scaladsl.Sink
import com.emarsys.rdb.connector.bigquery.BigQueryMetadata.TableDataQueryJsonProtocol.TableDataQueryResponse
import com.emarsys.rdb.connector.bigquery.BigQueryMetadata.TableListQueryJsonProtocol.TableListQueryResponse
import com.emarsys.rdb.connector.bigquery.stream.BigQueryStreamSource
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors.{ErrorWithMessage, TableNotFound}
import com.emarsys.rdb.connector.common.models.TableSchemaDescriptors.{FieldModel, FullTableModel, TableModel}
import spray.json.{DefaultJsonProtocol, JsObject, JsonFormat}

import scala.concurrent.Future

trait BigQueryMetadata {
  self: BigQueryConnector =>

  protected def runMetaQuery[T](url: String, parser: JsObject => Seq[T]): ConnectorResponse[Seq[T]] = {
    val request = HttpRequest(HttpMethods.GET, url)
    val bigQuerySource = BigQueryStreamSource(request, parser, googleTokenActor, Http())

    bigQuerySource.runWith(Sink.seq)
      .map(listOfList => Right(listOfList.flatten))
      .recover{case x: Throwable => Left(ErrorWithMessage(x.getMessage))}
  }

  private def parseTableResult(result: JsObject): Seq[TableModel] = {
    result.convertTo[TableListQueryResponse].toTableModelList
  }

  private def parseFieldResults(result: JsObject): Seq[FieldModel] = {
    result.convertTo[TableDataQueryResponse].toFieldModelList
  }


  override def listTables(): ConnectorResponse[Seq[TableModel]] = {
    val url = s"https://www.googleapis.com/bigquery/v2/projects/${config.projectId}/datasets/${config.dataset}/tables"
    runMetaQuery(url, parseTableResult)
  }

  override def listFields(tableName: String): ConnectorResponse[Seq[FieldModel]] = {
    val url = s"https://www.googleapis.com/bigquery/v2/projects/${config.projectId}/datasets/${config.dataset}/tables/$tableName"
    runMetaQuery(url, parseFieldResults).map{
      case Left(x) => Left(TableNotFound(tableName))
      case x => x
    }
  }

  override def listTablesWithFields(): ConnectorResponse[Seq[FullTableModel]] = {
    for {
      tablesE <- listTables()
      f <- Future.sequence{
        tablesE match {
          case Right(tableList) => tableList.map(table => listFields(table.name).map(_.map(fieldList => (table.name, fieldList))))
          case Left(error) => Seq(Future.successful(Left(error)))
        }
      }
      mapE = sequence(f).map(_.toMap)
    } yield tablesE.flatMap(tables => mapE.map(map => makeTablesWithFields(tables, map)))
  }

  def sequence[A, B](s: Seq[Either[A, B]]): Either[A, Seq[B]] =
    s.foldRight(Right(Nil): Either[A, List[B]]) {
      (e, acc) => for (xs <- acc.right; x <- e.right) yield x :: xs
    }

  private def makeTablesWithFields(tableList: Seq[TableModel], tableFieldMap: Map[String, Seq[FieldModel]]): Seq[FullTableModel] = {
    tableList.map(table => FullTableModel(table.name, table.isView, tableFieldMap(table.name)))
  }
}

object BigQueryMetadata {

  object TableListQueryJsonProtocol extends DefaultJsonProtocol {

    case class TableListQueryResponse(tables: Seq[QueryTableModel]) {
      def toTableModelList: Seq[TableModel] = {
        tables.map(_.toTableModel)
      }
    }
    case class QueryTableModel(tableReference: TableReference, `type`: String){
      def toTableModel: TableModel = {
        TableModel(tableReference.tableId, `type` == "VIEW")
      }
    }
    case class TableReference(tableId: String)


    implicit val tableReferenceFormat: JsonFormat[TableReference] = jsonFormat1(TableReference)
    implicit val queryTableModelFormat: JsonFormat[QueryTableModel] = jsonFormat2(QueryTableModel)
    implicit val tableListQueryResponseFormat: JsonFormat[TableListQueryResponse] = jsonFormat1(TableListQueryResponse)
  }
  object TableDataQueryJsonProtocol extends DefaultJsonProtocol {
    case class TableDataQueryResponse(schema: TableSchema) {
      def toFieldModelList = {
        schema.fields.map(_.toFieldModel)
      }
    }
    case class TableSchema(fields: Seq[Field])
    case class Field(name: String, `type`: String) {
      def toFieldModel = {
        FieldModel(name, `type`)
      }
    }

    implicit val fieldFormat: JsonFormat[Field] = jsonFormat2(Field)
    implicit val tableSchemaFormat: JsonFormat[TableSchema] = jsonFormat1(TableSchema)
    implicit val tableDataQueryResponseFormat: JsonFormat[TableDataQueryResponse] = jsonFormat1(TableDataQueryResponse)
  }

}