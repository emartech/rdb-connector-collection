package com.emarsys.rdb.connector.bigquery

import akka.NotUsed
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest}
import akka.stream.scaladsl.{Flow, Source}
import com.emarsys.rdb.connector.bigquery.BigQueryStreamingQuery.DryRunJsonProtocol.DryRunResponse
import com.emarsys.rdb.connector.bigquery.GoogleApi.queryUrl
import com.emarsys.rdb.connector.bigquery.stream.BigQueryStreamSource
import com.emarsys.rdb.connector.common.ConnectorResponse
import spray.json.{DefaultJsonProtocol, JsObject, JsString, JsValue, JsonFormat, pimpAny}

import scala.concurrent.Future

trait BigQueryStreamingQuery {
  self: BigQueryConnector =>

  import BigQueryStreamingQuery.QueryJsonProtocol._

  protected def streamingQuery(query: String): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    val queryReq = QueryRequest(query)
    val request = createQueryRequest(queryReq, config.projectId)
    val bigQuerySource = BigQueryStreamSource(request, parseResult, googleTokenActor, Http()).via(concatWitFieldNamesAsFirst)
    Future.successful(Right(bigQuerySource))
  }

  protected def streamingDryQuery(query: String): Source[Seq[String], NotUsed] = {
    val queryReq = QueryRequest(query, dryRun = Option(true))
    val request = createQueryRequest(queryReq, config.projectId)
    val bigQuerySource = BigQueryStreamSource(request, parseDryResult, googleTokenActor, Http())
    bigQuerySource.mapConcat(identity)
  }

  private val concatWitFieldNamesAsFirst =
    Flow[(Seq[String], Seq[Seq[String]])].statefulMapConcat(() => {
      var isFirstRun = true

      {
        case (fields, rows) =>
          if (isFirstRun && rows.nonEmpty) {
            isFirstRun = false
            fields :: rows.toList
          } else {
            rows.toList
          }
      }
    })

  private def createQueryRequest(query: QueryRequest, projectId: String) =
    HttpRequest(HttpMethods.POST, queryUrl(projectId), entity = createQueryBody(query))

  private def createQueryBody(query: QueryRequest) =
    HttpEntity(ContentTypes.`application/json`, query.toJson.compactPrint)

  private def parseResult(result: JsObject): (Seq[String], Seq[Seq[String]]) = {
    val queryResponse = result.convertTo[QueryResponse]

    val fields = queryResponse.schema.fields.map(_.name)
    val rows = queryResponse.rows.fold(Seq[Seq[String]]())(rowSeq => rowSeq.map(row => row.f.map(_.v)))

    (fields, rows)
  }

  private def parseDryResult(result: JsObject): List[Seq[String]] = {
    val queryResponse = result.convertTo[DryRunResponse]

    val fields = Seq("totalBytesProcessed", "jobComplete", "cacheHit")
    val row = Seq(queryResponse.totalBytesProcessed, queryResponse.jobComplete.toString, queryResponse.cacheHit.toString)

    List(fields, row)
  }

}

object BigQueryStreamingQuery {

  object DryRunJsonProtocol extends DefaultJsonProtocol {

    case class DryRunResponse(totalBytesProcessed: String, jobComplete: Boolean, cacheHit: Boolean)

    implicit val dryRunFormat: JsonFormat[DryRunResponse] = jsonFormat3(DryRunResponse)
  }

  object QueryJsonProtocol extends DefaultJsonProtocol {

    case class QueryRequest(query: String, useLegacySql: Boolean = false, maxResults: Option[Int] = None, dryRun: Option[Boolean] = None)

    case class QueryResponse(schema: Schema, rows: Option[Seq[Row]])

    case class Schema(fields: Seq[FieldSchema])

    case class FieldSchema(name: String)

    case class Row(f: Seq[RowValue])

    case class RowValue(v: String)

    implicit val queryRequestFormat: JsonFormat[QueryRequest] = jsonFormat4(QueryRequest)

    implicit val rowValiueFormat: JsonFormat[RowValue] = new JsonFormat[RowValue] {
      override def write(obj: RowValue): JsValue = JsObject("v" -> JsString(obj.v))

      override def read(json: JsValue): RowValue = json.asJsObject.getFields("v") match {
        case Seq(JsString(value)) => RowValue(value)
        case Seq(value) => RowValue(value.toString)
        case _ => RowValue(json.toString)
      }
    }
    implicit val rowFormat: JsonFormat[Row] = jsonFormat1(Row)
    implicit val fieldFormat: JsonFormat[FieldSchema] = jsonFormat1(FieldSchema)
    implicit val schemaFormat: JsonFormat[Schema] = jsonFormat1(Schema)
    implicit val queryResponseFormat: JsonFormat[QueryResponse] = jsonFormat2(QueryResponse)

  }

}
