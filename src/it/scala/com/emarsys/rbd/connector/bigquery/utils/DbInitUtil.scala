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
import concurrent.duration._
trait DbInitUtil {
  implicit val sys: ActorSystem
  implicit val materializer: ActorMaterializer
  implicit val timeout: Timeout
  implicit lazy val ec = sys.dispatcher

  lazy val connector: BigQueryConnector = Await.result(BigQueryConnector(TestHelper.TEST_CONNECTION_CONFIG)(sys), timeout.duration).right.get

  def sleep() = {
    Await.result(Future(Thread.sleep(500)),1.seconds)
  }

  def runRequest(httpRequest: HttpRequest): Future[Done] = {
    val tokenActor = sys.actorOf(GoogleTokenActor.props(TestHelper.TEST_CONNECTION_CONFIG.clientEmail, TestHelper.TEST_CONNECTION_CONFIG.privateKey, Http()))
    BigQueryStreamSource(httpRequest, identity, tokenActor, Http()).runWith(Sink.ignore)
  }

  def createTable(schemaDefinition: String): HttpRequest = HttpRequest(
    HttpMethods.POST,
    Uri(connector.createTableUrl),
    entity = HttpEntity(ContentTypes.`application/json`, schemaDefinition)
  )

  def insertInto(data: String, table: String) = HttpRequest(
    HttpMethods.POST,
    Uri(connector.insertIntoUrl(table)),
    entity = HttpEntity(ContentTypes.`application/json`, data)
  )

  def dropTable(table: String) = HttpRequest(
    HttpMethods.DELETE,
    Uri(connector.deleteTableUrl(table))
  )
}
