package com.emarsys.rdb.connector.bigquery

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, HttpRequest}
import akka.stream.scaladsl.Sink
import cats.data.EitherT
import cats.implicits._
import cats.{Applicative, Monad}
import com.emarsys.rdb.connector.bigquery.BigQueryMetadata.TableDataQueryJsonProtocol.TableDataQueryResponse
import com.emarsys.rdb.connector.bigquery.BigQueryMetadata.TableListQueryJsonProtocol.TableListQueryResponse
import com.emarsys.rdb.connector.bigquery.GoogleApi.{fieldListUrl, tableListUrl}
import com.emarsys.rdb.connector.bigquery.stream.BigQueryStreamSource
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors.{ErrorWithMessage, TableNotFound}
import com.emarsys.rdb.connector.common.models.TableSchemaDescriptors.{FieldModel, FullTableModel, TableModel}
import spray.json.{DefaultJsonProtocol, JsObject, JsonFormat}

trait BigQueryMetadata {
  self: BigQueryConnector =>

  protected def runMetaQuery[T](url: String, parser: JsObject => Seq[T]): ConnectorResponse[Seq[T]] = {
    val request = HttpRequest(HttpMethods.GET, url)
    val bigQuerySource = BigQueryStreamSource(request, parser, googleTokenActor, Http())

    bigQuerySource.runWith(Sink.seq)
      .map(listOfList => Right(listOfList.flatten))
      .recover { case ex: Throwable => Left(ErrorWithMessage(ex.getMessage)) }
  }

  private def parseTableResult(result: JsObject): Seq[TableModel] = {
    result.convertTo[TableListQueryResponse].toTableModelList
  }

  private def parseFieldResults(result: JsObject): Seq[FieldModel] = {
    result.convertTo[TableDataQueryResponse].toFieldModelList
  }


  override def listTables(): ConnectorResponse[Seq[TableModel]] = {
    runMetaQuery(tableListUrl(config.projectId, config.dataset), parseTableResult)
  }

  override def listFields(tableName: String): ConnectorResponse[Seq[FieldModel]] = {
    runMetaQuery(fieldListUrl(config.projectId, config.dataset, tableName), parseFieldResults).map {
      case Left(ErrorWithMessage(message))
        if message.startsWith("Unexpected error in response: 404 Not Found") =>
          Left(TableNotFound(tableName))
      case other   => other
    }
  }

  override def listTablesWithFields(): ConnectorResponse[Seq[FullTableModel]] = {
    for {
      tables <- EitherT(listTables())
      map <- sequence(tables.map(table => EitherT(listFields(table.name)).map(fieldModel => (table.name, fieldModel))))
    } yield makeTablesWithFields(tables, map.toMap)
  }.value

  def sequence[F[_] : Applicative : Monad, A, B](s: Seq[EitherT[F, A, B]]): EitherT[F, A, Seq[B]] =
    s.foldRight(EitherT.rightT[F, A](Nil: Seq[B])) {
      (e, acc) => for (xs <- acc; x <- e) yield x +: xs
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

    case class QueryTableModel(tableReference: TableReference, `type`: String) {
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
