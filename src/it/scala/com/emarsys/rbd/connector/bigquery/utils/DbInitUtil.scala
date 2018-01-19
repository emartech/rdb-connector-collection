package com.emarsys.rbd.connector.bigquery.utils

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import com.emarsys.rdb.connector.bigquery.GoogleApi._
import com.emarsys.rdb.connector.bigquery.stream.BigQueryStreamSource
import com.emarsys.rdb.connector.bigquery.{BigQueryConnector, GoogleTokenActor}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

trait DbInitUtil {
  implicit val sys: ActorSystem
  implicit val materializer: ActorMaterializer
  implicit val timeout: Timeout
  implicit lazy val ec: ExecutionContext = sys.dispatcher

  private val testConfig = TestHelper.TEST_CONNECTION_CONFIG

  lazy val connector: BigQueryConnector = Await.result(BigQueryConnector(testConfig)(sys), timeout.duration).right.get

  def sleep(): Unit = {
    Await.result(Future(Thread.sleep(500)), 1.seconds)
  }

  def runRequest(httpRequest: HttpRequest): Future[Done] = {
    val tokenActor = sys.actorOf(GoogleTokenActor.props(testConfig.clientEmail, testConfig.privateKey, Http()))
    BigQueryStreamSource(httpRequest, identity, tokenActor, Http()).runWith(Sink.ignore)
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

  def dropTable(table: String) = HttpRequest(
    HttpMethods.DELETE,
    Uri(deleteTableUrl(testConfig.projectId, testConfig.dataset, table))
  )
}
