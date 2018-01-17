package com.emarsys.rdb.connector.bigquery

import akka.NotUsed
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest}
import akka.stream.scaladsl.{Flow, Source}
import com.emarsys.rdb.connector.bigquery.stream.BigQueryStreamSource
import com.emarsys.rdb.connector.common.ConnectorResponse
import spray.json._
import fommil.sjs.FamilyFormats._

import scala.collection.immutable
import scala.concurrent.Future

final case class QueryRequest(query: String, useLegacySql: Boolean = false, maxResults: Option[Int] = None)

trait BigQueryStreamingQuery {
  self: BigQueryConnector =>

  protected def streamingQuery(query: String): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    val projectId: String = config.projectId

    val gta = actorSystem.actorOf(GoogleTokenActor.props(config.clientEmail, config.privateKey, Http()))
    val request = createQueryRequest(query, projectId)
    val bigQuerySource = BigQueryStreamSource(request, parseResult, gta, Http()).via(concatWitFieldNamesAsFirst)

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
    val fields = result.fields("schema").asJsObject().fields("fields").asInstanceOf[JsArray].elements.map(_.asJsObject.fields("name").asInstanceOf[JsString].value)

    val rows: immutable.Seq[JsObject] = result.fields.get("rows").fold(immutable.Seq[JsObject]())(_.asInstanceOf[JsArray].elements.map(_.asInstanceOf[JsObject]))

    (fields, rows.map(_.fields("f").asInstanceOf[JsArray]).map(_.elements.map(_.asJsObject.fields("v").toString())))
  }

}
