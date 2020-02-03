package com.emarsys.rbd.connector.bigquery.utils

import java.time.Clock

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import cats.syntax.option._
import com.emarsys.rdb.connector.bigquery.GoogleApi._
import com.emarsys.rdb.connector.bigquery.stream.BigQueryStreamSource
import com.emarsys.rdb.connector.bigquery.{BigQueryConnector, GoogleSession, GoogleTokenApi}

import scala.concurrent.{Await, ExecutionContext, Future}

trait DbInitUtil {
  implicit val sys: ActorSystem
  implicit val materializer: ActorMaterializer
  implicit val timeout: Timeout
  implicit lazy val ec: ExecutionContext = sys.dispatcher
  implicit val clock: Clock = java.time.Clock.systemUTC()

  private val testConfig = TestHelper.TEST_CONNECTION_CONFIG

  lazy val connector: BigQueryConnector = Await.result(BigQueryConnector(testConfig)(sys), timeout.duration).right.get

  def sleep(): Unit = Thread.sleep(1000)

  def runRequest(httpRequest: HttpRequest): Future[Done] = {
    val googleSession = new GoogleSession(testConfig.clientEmail, testConfig.privateKey, new GoogleTokenApi(Http()))
    BigQueryStreamSource(httpRequest, x => x.some, googleSession, Http()).runWith(Sink.ignore)
  }

  def createTable(schemaDefinition: String): HttpRequest = HttpRequest(
    HttpMethods.POST,
    Uri(createTableUrl(testConfig.projectId, testConfig.dataset)),
    entity = HttpEntity(ContentTypes.`application/json`, schemaDefinition)
  )

  def insertInto(data: String, table: String) = HttpRequest(
    HttpMethods.POST,
    Uri(insertIntoUrl(testConfig.projectId, testConfig.dataset, table)),
    entity = HttpEntity(ContentTypes.`application/json`, data)
  )

  def dropTable(table: String) =
    HttpRequest(HttpMethods.DELETE, Uri(deleteTableUrl(testConfig.projectId, testConfig.dataset, table)))
}
