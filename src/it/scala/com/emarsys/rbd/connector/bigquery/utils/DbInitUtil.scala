package com.emarsys.rbd.connector.bigquery.utils

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import com.emarsys.rdb.connector.bigquery.{BigQueryConnector, GoogleTokenActor}
import com.emarsys.rdb.connector.bigquery.stream.BigQueryStreamSource
import com.emarsys.rdb.connector.common.models.Connector

import scala.concurrent.{Await, Future}

trait DbInitUtil {
  implicit val sys: ActorSystem
  implicit val materializer: ActorMaterializer
  implicit val timeout: Timeout
  implicit lazy val ec = sys.dispatcher

  lazy val connector: Connector = Await.result(BigQueryConnector(TestHelper.TEST_CONNECTION_CONFIG)(sys), timeout.duration).right.get

  def runRequest(httpRequest: HttpRequest): Future[Done] = {
    val tokenActor = sys.actorOf(GoogleTokenActor.props(TestHelper.TEST_CONNECTION_CONFIG.clientEmail, TestHelper.TEST_CONNECTION_CONFIG.privateKey, Http()))
    BigQueryStreamSource(httpRequest, identity, tokenActor, Http()).runWith(Sink.ignore)
  }

  def createTable(schemaDefinition: String): HttpRequest = HttpRequest(
    HttpMethods.POST,
    Uri(s"https://www.googleapis.com/bigquery/v2/projects/${TestHelper.TEST_CONNECTION_CONFIG.projectId}/datasets/${TestHelper.TEST_CONNECTION_CONFIG.dataset}/tables"),
    entity = HttpEntity(ContentTypes.`application/json`, schemaDefinition)
  )

  def insertInto(data: String, table: String) = HttpRequest(
    HttpMethods.POST,
    Uri(s"https://www.googleapis.com/bigquery/v2/projects/${TestHelper.TEST_CONNECTION_CONFIG.projectId}/datasets/${TestHelper.TEST_CONNECTION_CONFIG.dataset}/tables/$table/insertAll"),
    entity = HttpEntity(ContentTypes.`application/json`, data)
  )

  def dropTable(table: String) = HttpRequest(
    HttpMethods.DELETE,
    Uri(s"https://www.googleapis.com/bigquery/v2/projects/${TestHelper.TEST_CONNECTION_CONFIG.projectId}/datasets/${TestHelper.TEST_CONNECTION_CONFIG.dataset}/tables/$table")
  )
}
