package com.emarsys.rdb.connector.bigquery

import akka.NotUsed
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest}
import akka.stream.scaladsl.{Flow, Source}
import com.emarsys.rdb.connector.bigquery.stream.BigQueryStreamSource
import com.emarsys.rdb.connector.common.ConnectorResponse
import spray.json.{DefaultJsonProtocol, JsObject, JsString, JsValue, JsonFormat, RootJsonFormat, pimpAny}

import scala.concurrent.Future

trait BigQueryStreamingQuery {
  self: BigQueryConnector =>
  import BigQueryStreamingQuery.QueryJsonProtocol._

  protected def streamingQuery(query: String): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    val projectId: String = config.projectId

    val request = createQueryRequest(query, projectId)
    val bigQuerySource = BigQueryStreamSource(request, parseResult, googleTokenActor, Http()).via(concatWitFieldNamesAsFirst)

    Future.successful(Right(bigQuerySource))
  }

  private val concatWitFieldNamesAsFirst =
    Flow[(Seq[String], Seq[Seq[String]])].statefulMapConcat(() => {
      var state = true

      {
        case (fields, rows) =>
          if (state && rows.nonEmpty) {
            state = false
            fields :: rows.toList
          } else {
            rows.toList
          }
      }
    })

  private def getQueryUrl(projectId: String) = s"https://www.googleapis.com/bigquery/v2/projects/$projectId/queries"

  private def createQueryRequest(query: String, projectId: String) =
    HttpRequest(HttpMethods.POST, getQueryUrl(projectId), entity = createQueryBody(query))

  private def createQueryBody(query: String) =
    HttpEntity(ContentTypes.`application/json`, QueryRequest(query).toJson.compactPrint)

  private def parseResult(result: JsObject): (Seq[String], Seq[Seq[String]]) = {
    val queryResponse = result.convertTo[QueryResponse]

    val fields = queryResponse.schema.fields.map(_.name)
    val rows = queryResponse.rows.fold(Seq[Seq[String]]())(rowSeq => rowSeq.map(row => row.f.map(_.v)))

    (fields, rows)
  }

}

object BigQueryStreamingQuery {
  object QueryJsonProtocol extends DefaultJsonProtocol {

    case class QueryRequest(query: String, useLegacySql: Boolean = false, maxResults: Option[Int] = None)
    case class QueryResponse(schema: Schema, rows: Option[Seq[Row]])
    case class Schema(fields: Seq[FieldSchema])
    case class FieldSchema(name: String)
    case class Row(f: Seq[RowValue])
    case class RowValue(v: String)

    implicit val queryRequestFormat: JsonFormat[QueryRequest] = jsonFormat3(QueryRequest)

    implicit val rowValiueFormat: JsonFormat[RowValue] = new JsonFormat[RowValue] {
      override def write(obj: RowValue): JsValue = JsObject("v" -> JsString(obj.v))

      override def read(json: JsValue): RowValue = RowValue(json.toString())
    }
    implicit val rowFormat: JsonFormat[Row] = jsonFormat1(Row)
    implicit val fieldFormat: JsonFormat[FieldSchema] = jsonFormat1(FieldSchema)
    implicit val schemaFormat: JsonFormat[Schema] = jsonFormat1(Schema)
    implicit val queryResponseFormat: JsonFormat[QueryResponse] = jsonFormat2(QueryResponse)

  }

}
