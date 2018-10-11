package com.emarsys.rdb.connector.test

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl.Source
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.{Connector, SimpleSelect}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.mockito.MockitoSugar

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration

class SelectWithGroupLimitItSpecSpec extends TestKit(ActorSystem()) with SelectWithGroupLimitItSpec with MockitoSugar with BeforeAndAfterAll {

  implicit val materializer: Materializer = ActorMaterializer()

  implicit val executionContext = system.dispatcher

  override val connector = new Connector {
    override implicit val executionContext: ExecutionContext = system.dispatcher


    override def simpleSelect(select: SimpleSelect, timeout: FiniteDuration): ConnectorResponse[Source[Seq[String], NotUsed]] = {
      Future(Right(Source(List(
        Seq("ID", "NAME", "DATA"),
        Seq("1", "test1", "data1"),
        Seq("1", "test1", "data2")
      ))))
    }


    override protected def runSelectWithGroupLimit(select: SimpleSelect, groupLimit: Int, references: Seq[String], timeout: FiniteDuration): ConnectorResponse[Source[Seq[String], NotUsed]] = {
      if(references.size == 2) {
        Future(Right(Source(List(
          Seq("ID", "NAME", "DATA"),
          Seq("1", "test1", "data1"),
          Seq("1", "test1", "data2"),
          Seq("2", "test2", "data5"),
          Seq("2", "test2", "data6"),
          Seq("2", "test3", "data8")
        ))))
      } else {
        Future(Right(Source(List())))
      }
    }

    override def close(): Future[Unit] = ???
  }

  override def initDb(): Unit = ()

  override def cleanUpDb(): Unit = ()

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

}
